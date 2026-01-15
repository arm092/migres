"""
MS Teams Notification System for CDC
Sends notifications to MS Teams channels for errors, warnings, and important events
"""

import json
import logging
import sys
import traceback
from datetime import datetime
from enum import Enum
from typing import Dict, Optional, Tuple

import requests

log = logging.getLogger(__name__)


class NotificationLevel(Enum):
    """Notification severity levels"""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


def _extract_exception_info(exc: Optional[Exception] = None, tb=None) -> Dict[str, str]:
    """
    Extract exception location information (filename, line number, function name)
    Finds the first frame from user's codebase (not vendor libraries)
    
    Args:
        exc: Exception object (optional)
        tb: Traceback object (optional, if None will use current exception)
        
    Returns:
        Dictionary with exception location details
    """
    location_info = {}
    
    try:
        if tb is None:
            # Get current exception traceback
            exc_type, exc_value, exc_tb = sys.exc_info()
            if exc_tb is not None:
                tb = exc_tb
            elif exc is not None:
                # Try to get traceback from exception
                tb = exc.__traceback__
        
        if tb is not None:
            import os
            
            # Collect all frames from user's codebase (not just the first one)
            # This gives us more context about where the error occurred
            user_frames = []
            frame = tb
            
            while frame is not None:
                frame_info = frame.tb_frame
                filename = frame_info.f_code.co_filename
                
                # Check if this is user's code (not vendor code)
                # Skip: site-packages, dist-packages, clickhouse_driver, .pyx files (Cython)
                if (not any(skip in filename for skip in ['site-packages', 'dist-packages', 'clickhouse_driver', '.pyx']) 
                    and filename.endswith('.py')):
                    filename_short = os.path.basename(filename)
                    line_no = frame.tb_lineno
                    function_name = frame_info.f_code.co_name
                    user_frames.append((filename_short, line_no, function_name))
                
                frame = frame.tb_next
            
            # If we found user frames, use them
            if user_frames:
                # Primary location (first user frame - where error was raised)
                primary = user_frames[0]
                location_info["Exception Location"] = f"{primary[0]}:{primary[1]} in {primary[2]}()"
                
                # If there are multiple frames, add context (shows call chain)
                if len(user_frames) > 1:
                    context_frames = [f"{f[0]}:{f[1]} in {f[2]}()" for f in user_frames[1:3]]  # Max 2 additional frames
                    if context_frames:
                        location_info["Call Chain"] = " → ".join(context_frames)
            
    except Exception as e:
        log.debug(f"Failed to extract exception info: {e}")
    
    # Return empty dict if extraction failed
    return location_info


def _get_environment_color(environment: str) -> str:
    """
    Get color based on environment value (case-insensitive)
    
    Args:
        environment: Environment name (dev, stage, prod, production)
        
    Returns:
        Color name for Adaptive Card: "Good" (green), "Warning" (yellow), or "Attention" (red)
    """
    env_lower = environment.lower()
    
    if env_lower == "dev":
        return "Good"  # Green
    elif env_lower == "stage":
        return "Warning"  # Yellow/Orange
    elif env_lower in ("prod", "production"):
        return "Attention"  # Red
    else:
        # Default to green for unknown environments
        return "Good"


def _create_adaptive_card(title: str, message: str, level: NotificationLevel,
                          details: Optional[Dict] = None, environment: str = "prod") -> Dict:
    """Create MS Teams adaptive card payload"""

    # Remove emojis from title (MS Teams webhooks can have issues with emojis in adaptive cards)
    # Keep emojis for display but use clean text for adaptive card
    clean_title = title
    # Optionally strip emojis if needed, but let's try keeping them first

    # Color coding based on environment (not level)
    color = _get_environment_color(environment)

    # Create adaptive card body
    body = [
        {
            "type": "TextBlock",
            "text": clean_title,
            "weight": "Bolder",
            "size": "Medium",
            "color": color,
            "wrap": True
        },
        {
            "type": "TextBlock",
            "text": message,
            "wrap": True,
            "spacing": "Medium"
        },
        {
            "type": "TextBlock",
            "text": f"Level: {level.value.upper()}",
            "spacing": "Small",
            "isSubtle": True
        },
        {
            "type": "TextBlock",
            "text": f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}",
            "spacing": "Small",
            "isSubtle": True
        }
    ]

    # Add details section if provided - each detail as separate TextBlock
    if details:
        # Add separator before details
        body.append({
            "type": "TextBlock",
            "text": "---",
            "spacing": "Medium",
            "separator": True
        })
        
        # Add each detail as a separate TextBlock for better formatting
        for key, value in details.items():
            if isinstance(value, (dict, list)):
                value_str = json.dumps(value, indent=2)
            else:
                value_str = str(value)
            # Truncate long values to avoid payload size issues
            if len(value_str) > 500:
                value_str = value_str[:500] + "... (truncated)"
            
            body.append({
                "type": "TextBlock",
                "text": f"**{key}:** {value_str}",
                "wrap": True,
                "spacing": "Small"
            })

    # Create adaptive card
    card = {
        "type": "message",
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": {
                    "type": "AdaptiveCard",
                    "version": "1.2",
                    "body": body
                }
            }
        ]
    }

    return card


class TeamsNotification:
    """MS Teams notification handler"""

    def __init__(self, webhook_url: str, enabled: bool = True, rate_limit_seconds: int = 60, environment: str = "prod"):
        """
        Initialize MS Teams notification handler

        Args:
            webhook_url: MS Teams webhook URL
            enabled: Whether notifications are enabled
            rate_limit_seconds: Minimum seconds between notifications (0 = no limit)
            environment: Environment name (e.g., "dev", "prod") - will be shown in titles as [ENVIRONMENT]
        """
        self.webhook_url = webhook_url
        self.enabled = enabled
        self.rate_limit_seconds = rate_limit_seconds
        self.environment = environment.upper()
        self.last_notification_time = {}

    def _format_title(self, title: str) -> str:
        """Format notification title with environment tag"""
        return f"{title} [{self.environment}]"

    def _should_send_notification(self, notification_type: str) -> bool:
        """Check if notification should be sent based on rate limiting"""
        if not self.enabled:
            return False

        if self.rate_limit_seconds <= 0:
            return True

        now = datetime.now()
        last_time = self.last_notification_time.get(notification_type)

        if last_time is None:
            self.last_notification_time[notification_type] = now
            return True

        time_diff = (now - last_time).total_seconds()
        if time_diff >= self.rate_limit_seconds:
            self.last_notification_time[notification_type] = now
            return True

        return False

    def send_notification(self, title: str, message: str, level: NotificationLevel = NotificationLevel.INFO,
                        details: Optional[Dict] = None, notification_type: str = "general") -> bool:
        """
        Send notification to MS Teams
        
        Args:
            title: Notification title
            message: Notification message
            level: Notification severity level
            details: Additional details dictionary
            notification_type: Type of notification for rate limiting
            
        Returns:
            bool: True if notification was sent successfully
        """
        if not self._should_send_notification(notification_type):
            log.debug(f"Notification rate limited for type: {notification_type}")
            return False
            
        try:
            # Create adaptive card payload (use environment for color, not level)
            payload = _create_adaptive_card(title, message, level, details, self.environment)
            # Send to MS Teams
            response = requests.post(
                self.webhook_url,
                json=payload,
                timeout=10
            )
            
            # MS Teams webhooks return 200 with body "1" on success
            # They can also return 200 with error messages, so check the body
            response_text = response.text.strip() if response.text else ""

            if response.status_code == 200:
                # Check if response indicates success (MS Teams returns "1" on success)
                if response_text == "1":
                    log.info(f"MS Teams notification sent successfully: {title}")
                    return True
                else:
                    # HTTP 200 but body indicates an error
                    error_msg = f"MS Teams webhook returned 200 but with error response: {response_text}"
                    log.error(error_msg)
                    log.debug(f"Full response: status={response.status_code}, headers={response.headers}, body={response_text}")
                    return False
            else:
                error_msg = f"Failed to send MS Teams notification. Status: {response.status_code}"
                if response.status_code == 404:
                    error_msg += " (Webhook URL not found - check if URL is correct)"
                elif response.status_code == 400:
                    error_msg += " (Bad request - check payload format)"
                elif response.status_code == 401:
                    error_msg += " (Unauthorized - check webhook permissions)"
                else:
                    error_msg += f", Response: {response_text}"
                
                log.error(error_msg)
                return False
                
        except requests.exceptions.RequestException as e:
            log.error(f"Error sending MS Teams notification: {e}")
            return False
        except (TypeError, ValueError) as e:
            log.error(f"Unexpected error sending MS Teams notification: {e}")
            return False
    
    def send_cdc_error(self, error_type: str, table: str, error_message: str, 
                      operation_details: Optional[Dict] = None, exc: Optional[Exception] = None) -> bool:
        """
        Send CDC error notification
        
        Args:
            error_type: Type of error
            table: Table name
            error_message: Error message
            operation_details: Additional operation details
            exc: Exception object (optional, used to extract location info)
        """
        title = self._format_title(f"🚨 CDC Error: {error_type}")
        message = f"**Table:** {table}\n**Error:** {error_message}"
        
        details = {
            "Error Type": error_type,
            "Table": table,
            "Error Message": error_message
        }
        
        # Add exception location information (only if found)
        location_info = {}
        if exc is not None:
            location_info = _extract_exception_info(exc)
        else:
            # Try to get current exception info if available
            try:
                location_info = _extract_exception_info()
            except Exception:
                pass
        
        # Only add location if we found it (avoid duplicates)
        if location_info:
            details.update(location_info)
        
        if operation_details:
            details.update(operation_details)
            
        return self.send_notification(
            title=title,
            message=message,
            level=NotificationLevel.ERROR,
            details=details,
            notification_type="cdc_error"
        )
    
    def send_cdc_warning(self, warning_type: str, table: str, warning_message: str,
                        details: Optional[Dict] = None) -> bool:
        """Send CDC warning notification"""
        title = self._format_title(f"⚠️ CDC Warning: {warning_type}")
        message = f"**Table:** {table}\n**Warning:** {warning_message}"
        
        notification_details = {
            "Warning Type": warning_type,
            "Table": table,
            "Warning Message": warning_message
        }
        
        if details:
            notification_details.update(details)
            
        return self.send_notification(
            title=title,
            message=message,
            level=NotificationLevel.WARNING,
            details=notification_details,
            notification_type="cdc_warning"
        )
    
    def send_cdc_info(self, info_type: str, message: str, details: Optional[Dict] = None) -> bool:
        """Send CDC info notification"""
        title = self._format_title(f"ℹ️ CDC Info: {info_type}")
        
        notification_details = {
            "Info Type": info_type,
            "Message": message
        }
        
        if details:
            notification_details.update(details)
            
        return self.send_notification(
            title=title,
            message=message,
            level=NotificationLevel.INFO,
            details=notification_details,
            notification_type="cdc_info"
        )
    
    def send_cdc_startup(self, config_summary: Dict, mode: str = "cdc") -> bool:
        """Send CDC startup notification"""
        if mode.lower() == "snapshot":
            title = self._format_title("📸 Snapshot Started")
            message = "Snapshot migration process has started successfully"
        else:
            title = self._format_title("🚀 CDC Started")
            message = "CDC (Change Data Capture) process has started successfully"

        details = {
            "Startup Time": datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC'),
            "Configuration": config_summary
        }
        
        return self.send_notification(
            title=title,
            message=message,
            level=NotificationLevel.INFO,
            details=details,
            notification_type="cdc_startup"
        )
    
    def send_cdc_shutdown(self, reason: str = "Normal shutdown") -> bool:
        """Send CDC shutdown notification"""
        title = self._format_title("🛑 CDC Process Stopped")
        message = f"CDC process has stopped. Reason: {reason}"
        
        details = {
            "Shutdown Time": datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC'),
            "Reason": reason
        }
        
        return self.send_notification(
            title=title,
            message=message,
            level=NotificationLevel.WARNING,
            details=details,
            notification_type="cdc_shutdown"
        )


def create_notification_handler(config: Dict, environment: str = "prod") -> Optional[TeamsNotification]:
    """
    Create notification handler from configuration
    
    Args:
        config: Notification configuration dictionary
        environment: Environment name (default: "prod")

    Returns:
        TeamsNotification instance or None if disabled
    """
    if not config.get("enabled", False):
        log.info("Notifications are disabled in configuration")
        return None
        
    webhook_url = config.get("webhook_url")
    if not webhook_url:
        log.error("MS Teams webhook URL not configured")
        return None
        
    rate_limit = config.get("rate_limit_seconds", 60)
    
    return TeamsNotification(
        webhook_url=webhook_url,
        enabled=True,
        rate_limit_seconds=rate_limit,
        environment=environment
    )


# Global notification handler instance
_notification_handler: Optional[TeamsNotification] = None


def initialize_notifications(config: Dict, environment: str = "prod") -> bool:
    """
    Initialize global notification handler
    
    Args:
        config: Notification configuration
        environment: Environment name (default: "prod")

    Returns:
        bool: True if initialized successfully
    """
    global _notification_handler
    
    try:
        _notification_handler = create_notification_handler(config, environment)
        if _notification_handler:
            log.info("MS Teams notifications initialized successfully")
            return True
        else:
            log.info("MS Teams notifications not configured or disabled")
            return False
    except Exception as e:
        log.error(f"Failed to initialize notifications: {e}")
        return False


def get_notification_handler() -> Optional[TeamsNotification]:
    """Get the global notification handler"""
    return _notification_handler


# Convenience functions for easy use throughout the codebase
def notify_cdc_error(error_type: str, table: str, error_message: str, 
                    operation_details: Optional[Dict] = None, exc: Optional[Exception] = None) -> bool:
    """
    Send CDC error notification
    
    Args:
        error_type: Type of error
        table: Table name
        error_message: Error message
        operation_details: Additional operation details
        exc: Exception object (optional, used to extract location info)
    """
    handler = get_notification_handler()
    if handler:
        return handler.send_cdc_error(error_type, table, error_message, operation_details, exc)
    return False


def notify_cdc_warning(warning_type: str, table: str, warning_message: str,
                      details: Optional[Dict] = None) -> bool:
    """Send CDC warning notification"""
    handler = get_notification_handler()
    if handler:
        return handler.send_cdc_warning(warning_type, table, warning_message, details)
    return False


def notify_cdc_info(info_type: str, message: str, details: Optional[Dict] = None) -> bool:
    """Send CDC info notification"""
    handler = get_notification_handler()
    if handler:
        return handler.send_cdc_info(info_type, message, details)
    return False


def notify_cdc_startup(config_summary: Dict, mode: str = "cdc") -> bool:
    """Send CDC startup notification"""
    handler = get_notification_handler()
    if handler:
        return handler.send_cdc_startup(config_summary, mode)
    return False


def notify_cdc_shutdown(reason: str = "Normal shutdown") -> bool:
    """Send CDC shutdown notification"""
    handler = get_notification_handler()
    if handler:
        return handler.send_cdc_shutdown(reason)
    return False
