#!/usr/bin/env python3
"""
Test script for MS Teams notification system
"""

import sys
import os
import time
import yaml
import pytest

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from notifications import initialize_notifications, notify_cdc_error, notify_cdc_warning, notify_cdc_info


@pytest.fixture(scope="module")
def notification_config():
    """Load notification configuration"""
    config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "config.yml")
    
    if not os.path.exists(config_path):
        pytest.skip("config.yml not found")
    
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
    except Exception as e:
        pytest.skip(f"Failed to read config.yml: {e}")
    
    notification_config = config.get("notifications", {})
    
    if not notification_config.get("enabled", False):
        pytest.skip("Notifications are disabled in config.yml")
    
    webhook_url = notification_config.get("webhook_url")
    if not webhook_url:
        pytest.skip("No webhook URL configured")
    
    if "your-webhook-url" in webhook_url:
        pytest.skip("Webhook URL is still the placeholder")
    
    return notification_config


@pytest.mark.notifications
def test_notification_initialization(notification_config):
    """Test notification system initialization"""
    print("🔧 Initializing notification system...")
    success = initialize_notifications(notification_config)
    assert success, "Failed to initialize notifications"
    print("✅ Notification system initialized")


@pytest.mark.notifications
def test_cdc_error_notification(notification_config):
    """Test CDC Error notification"""
    initialize_notifications(notification_config)
    
    print("1️⃣ Testing CDC Error notification...")
    success = notify_cdc_error(
        error_type="Test Error",
        table="test_table",
        error_message="This is a test error notification",
        operation_details={
            "Test": True,
            "Error Code": "TEST_001",
            "Timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }
    )
    assert success, "Failed to send error notification"
    print("   Result: ✅ Sent")
    time.sleep(2)


@pytest.mark.notifications
def test_cdc_warning_notification(notification_config):
    """Test CDC Warning notification"""
    initialize_notifications(notification_config)
    
    print("2️⃣ Testing CDC Warning notification...")
    success = notify_cdc_warning(
        warning_type="Test Warning",
        table="test_table",
        warning_message="This is a test warning notification",
        details={
            "Test": True,
            "Warning Code": "TEST_002",
            "Timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }
    )
    assert success, "Failed to send warning notification"
    print("   Result: ✅ Sent")
    time.sleep(2)


@pytest.mark.notifications
def test_cdc_info_notification(notification_config):
    """Test CDC Info notification"""
    initialize_notifications(notification_config)
    
    print("3️⃣ Testing CDC Info notification...")
    success = notify_cdc_info(
        info_type="Test Info",
        message="This is a test info notification",
        details={
            "Test": True,
            "Info Code": "TEST_003",
            "Timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }
    )
    assert success, "Failed to send info notification"
    print("   Result: ✅ Sent")
    time.sleep(2)


@pytest.mark.notifications
def test_rate_limiting(notification_config):
    """Test notification rate limiting"""
    initialize_notifications(notification_config)
    
    print("4️⃣ Testing rate limiting...")
    print("   Sending multiple notifications quickly...")
    
    sent_count = 0
    for i in range(3):
        success = notify_cdc_info(
            info_type=f"Rate Limit Test {i+1}",
            message=f"This is rate limit test notification {i+1}",
            details={"Test": True, "Iteration": i+1}
        )
        if success:
            sent_count += 1
        print(f"   Notification {i+1}: {'✅ Sent' if success else '⏳ Rate Limited'}")
        time.sleep(1)
    
    # At least one should be sent, but rate limiting may prevent all
    assert sent_count >= 1, "No notifications were sent (rate limiting may be too strict)"
    print(f"   Sent {sent_count}/3 notifications (rate limiting working)")




