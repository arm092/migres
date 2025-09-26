#!/usr/bin/env python3
"""
Test script for MS Teams notification system
"""

import sys
import os
import time
import json
import yaml

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from notifications import TeamsNotification, NotificationLevel, initialize_notifications, notify_cdc_error, notify_cdc_warning, notify_cdc_info


def test_notification_system():
    """Test the MS Teams notification system"""
    print("🧪 Testing MS Teams Notification System")
    print("=" * 50)
    
    # Load configuration from config.yml
    config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "config.yml")
    
    if not os.path.exists(config_path):
        print("❌ ERROR: config.yml not found!")
        print("   Please run this from the test/ directory")
        return False
    
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
    except Exception as e:
        print(f"❌ ERROR: Failed to read config.yml: {e}")
        return False
    
    # Get notification configuration
    notification_config = config.get("notifications", {})
    
    if not notification_config.get("enabled", False):
        print("❌ ERROR: Notifications are disabled in config.yml")
        print("   Set notifications.enabled: true in your config.yml")
        return False
    
    webhook_url = notification_config.get("webhook_url")
    if not webhook_url:
        print("❌ ERROR: No webhook URL configured!")
        print("   Add notifications.webhook_url to your config.yml")
        return False
    
    if "your-webhook-url" in webhook_url:
        print("❌ ERROR: Webhook URL is still the placeholder!")
        print("   Update notifications.webhook_url in your config.yml with a real webhook URL")
        return False
    
    print("📋 Test Configuration:")
    print(f"  - Enabled: {notification_config.get('enabled', False)}")
    print(f"  - Rate Limit: {notification_config.get('rate_limit_seconds', 60)}s")
    print(f"  - Webhook URL: {webhook_url[:50]}...")
    print()
    
    # Initialize notifications
    print("🔧 Initializing notification system...")
    success = initialize_notifications(notification_config)
    
    if not success:
        print("❌ Failed to initialize notifications")
        print("   Check your webhook URL in config.yml")
        return False
    
    print("✅ Notification system initialized")
    print()
    
    # Test different notification types
    print("📤 Sending test notifications...")
    
    # Test 1: CDC Error
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
    print(f"   Result: {'✅ Sent' if success else '❌ Failed'}")
    time.sleep(2)
    
    # Test 2: CDC Warning
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
    print(f"   Result: {'✅ Sent' if success else '❌ Failed'}")
    time.sleep(2)
    
    # Test 3: CDC Info
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
    print(f"   Result: {'✅ Sent' if success else '❌ Failed'}")
    time.sleep(2)
    
    # Test 4: Rate limiting
    print("4️⃣ Testing rate limiting...")
    print("   Sending multiple notifications quickly...")
    for i in range(3):
        success = notify_cdc_info(
            info_type=f"Rate Limit Test {i+1}",
            message=f"This is rate limit test notification {i+1}",
            details={"Test": True, "Iteration": i+1}
        )
        print(f"   Notification {i+1}: {'✅ Sent' if success else '⏳ Rate Limited'}")
        time.sleep(1)
    
    print()
    print("🎉 Notification testing completed!")
    print()
    print("📝 Next Steps:")
    print("1. Check your MS Teams channel for the test notifications")
    print("2. Configure notifications in your config.yml if not already done")
    print("3. Run the CDC process to see real notifications")
    
    return True


if __name__ == "__main__":
    print("🚀 MS Teams Notification System Test")
    print("=" * 50)
    print()
    print("This test will read your notification configuration from config.yml")
    print("Make sure you have configured notifications in your config.yml first.")
    print()
    
    # Ask user if they want to proceed with test
    response = input("Do you want to proceed with the notification test? (y/n): ").lower().strip()
    
    if response in ['y', 'yes']:
        test_notification_system()
    else:
        print("Test cancelled. Configure notifications in config.yml and run again when ready.")
