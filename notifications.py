"""
MS Teams Notification System for CDC
Sends notifications to MS Teams channels for errors, warnings, and important events
"""

import logging
import json
import requests
from datetime import datetime
from typing import Dict, List, Optional, Any
from enum import Enum

log = logging.getLogger(__name__)


class NotificationLevel(Enum):
    """Notification severity levels"""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class TeamsNotification:
    """MS Teams notification handler"""
    
    def __init__(self, webhook_url: str, enabled: bool = True, rate_limit_seconds: int = 60):
        """
        Initialize MS Teams notification handler
        
        Args:
            webhook_url: MS Teams webhook URL
            enabled: Whether notifications are enabled
            rate_limit_seconds: Minimum seconds between notifications (0 = no limit)
        """
        self.webhook_url = webhook_url
        self.enabled = enabled
        self.rate_limit_seconds = rate_limit_seconds
        self.last_notification_time = {}
        
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
    
    def _create_adaptive_card(self, title: str, message: str, level: NotificationLevel, 
                             details: Optional[Dict] = None) -> Dict:
        """Create MS Teams adaptive card payload"""
        
        # Color coding based on level
        color_map = {
            NotificationLevel.INFO: "00FF00",      # Green
            NotificationLevel.WARNING: "FFA500",  # Orange
            NotificationLevel.ERROR: "FF0000",    # Red
            NotificationLevel.CRITICAL: "8B0000"  # Dark Red
        }
        
        color = color_map.get(level, "00FF00")
        
        # Create adaptive card
        card = {
            "type": "message",
            "attachments": [
                {
                    "contentType": "application/vnd.microsoft.card.adaptive",
                    "content": {
                        "type": "AdaptiveCard",
                        "version": "1.3",
                        "body": [
                            {
                                "type": "TextBlock",
                                "text": title,
                                "weight": "Bolder",
                                "size": "Medium",
                                "color": "Default"
                            },
                            {
                                "type": "TextBlock",
                                "text": message,
                                "wrap": True,
                                "spacing": "Medium"
                            },
                            {
                                "type": "TextBlock",
                                "text": f"**Level:** {level.value.upper()}",
                                "spacing": "Small"
                            },
                            {
                                "type": "TextBlock",
                                "text": f"**Timestamp:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}",
                                "spacing": "Small"
                            }
                        ]
                    }
                }
            ]
        }
        
        # Add details section if provided
        if details:
            details_text = "**Details:**\n"
            for key, value in details.items():
                if isinstance(value, (dict, list)):
                    value = json.dumps(value, indent=2)
                details_text += f"- **{key}:** {value}\n"
            
            card["attachments"][0]["content"]["body"].append({
                "type": "TextBlock",
                "text": details_text,
                "wrap": True,
                "spacing": "Medium"
            })
        
        return card
    
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
            # Create adaptive card payload
            payload = self._create_adaptive_card(title, message, level, details)
            
            # Send to MS Teams
            response = requests.post(
                self.webhook_url,
                json=payload,
                timeout=10
            )
            
            if response.status_code == 200:
                log.info(f"MS Teams notification sent successfully: {title}")
                return True
            else:
                error_msg = f"Failed to send MS Teams notification. Status: {response.status_code}"
                if response.status_code == 404:
                    error_msg += " (Webhook URL not found - check if URL is correct)"
                elif response.status_code == 400:
                    error_msg += " (Bad request - check payload format)"
                elif response.status_code == 401:
                    error_msg += " (Unauthorized - check webhook permissions)"
                else:
                    error_msg += f", Response: {response.text}"
                
                log.error(error_msg)
                return False
                
        except requests.exceptions.RequestException as e:
            log.error(f"Error sending MS Teams notification: {e}")
            return False
        except Exception as e:
            log.error(f"Unexpected error sending MS Teams notification: {e}")
            return False
    
    def send_cdc_error(self, error_type: str, table: str, error_message: str, 
                      operation_details: Optional[Dict] = None) -> bool:
        """Send CDC error notification"""
        title = f"🚨 CDC Error: {error_type}"
        message = f"**Table:** {table}\n**Error:** {error_message}"
        
        details = {
            "Error Type": error_type,
            "Table": table,
            "Error Message": error_message
        }
        
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
        title = f"⚠️ CDC Warning: {warning_type}"
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
        title = f"ℹ️ CDC Info: {info_type}"
        
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
    
    def send_cdc_startup(self, config_summary: Dict) -> bool:
        """Send CDC startup notification"""
        title = "🚀 CDC Process Started"
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
        title = "🛑 CDC Process Stopped"
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


def create_notification_handler(config: Dict) -> Optional[TeamsNotification]:
    """
    Create notification handler from configuration
    
    Args:
        config: Notification configuration dictionary
        
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
        rate_limit_seconds=rate_limit
    )


# Global notification handler instance
_notification_handler: Optional[TeamsNotification] = None


def initialize_notifications(config: Dict) -> bool:
    """
    Initialize global notification handler
    
    Args:
        config: Notification configuration
        
    Returns:
        bool: True if initialized successfully
    """
    global _notification_handler
    
    try:
        _notification_handler = create_notification_handler(config)
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
                    operation_details: Optional[Dict] = None) -> bool:
    """Send CDC error notification"""
    handler = get_notification_handler()
    if handler:
        return handler.send_cdc_error(error_type, table, error_message, operation_details)
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


def notify_cdc_startup(config_summary: Dict) -> bool:
    """Send CDC startup notification"""
    handler = get_notification_handler()
    if handler:
        return handler.send_cdc_startup(config_summary)
    return False


def notify_cdc_shutdown(reason: str = "Normal shutdown") -> bool:
    """Send CDC shutdown notification"""
    handler = get_notification_handler()
    if handler:
        return handler.send_cdc_shutdown(reason)
    return False
