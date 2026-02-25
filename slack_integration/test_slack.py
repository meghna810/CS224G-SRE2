#!/usr/bin/env python3
"""
Test script for Slack integration.

Run this to verify your Slack webhook is configured correctly:
    python slack_integration/test_slack.py

Or test with a specific webhook URL:
    python slack_integration/test_slack.py https://hooks.slack.com/services/YOUR/WEBHOOK/URL
"""

import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from slack_integration.client import SlackNotifier


def test_basic_connection(webhook_url=None):
    """Test basic Slack connection."""
    print("=" * 80)
    print("🧪 Testing Slack Integration")
    print("=" * 80)

    # Create notifier
    if webhook_url:
        print(f"\n✅ Using webhook URL from command line")
        notifier = SlackNotifier(webhook_url=webhook_url, enabled=True)
    else:
        print(f"\n✅ Reading webhook URL from .env file")
        notifier = SlackNotifier()

    # Check configuration
    if not notifier.enabled:
        print("❌ Slack notifications are disabled")
        print("   Set SLACK_ENABLED=true in your .env file")
        return False

    if not notifier.webhook_url:
        print("❌ Slack webhook URL not configured")
        print("   Set SLACK_WEBHOOK_URL in your .env file")
        print("\n📖 To get a webhook URL:")
        print("   1. Go to https://api.slack.com/apps")
        print("   2. Create a new app or select an existing app")
        print("   3. Enable 'Incoming Webhooks' feature")
        print("   4. Add webhook to workspace and select channel")
        print("   5. Copy the webhook URL to your .env file")
        return False

    print(f"✅ Webhook URL configured: {notifier.webhook_url[:50]}...")

    # Test connection
    print("\n🔄 Testing connection...")
    if notifier.test_connection():
        print("✅ Connection test successful! Check your Slack channel.")
        return True
    else:
        print("❌ Connection test failed. Please check your webhook URL.")
        return False


def test_rca_notification(webhook_url=None):
    """Test RCA analysis notification."""
    print("\n" + "=" * 80)
    print("🧪 Testing RCA Analysis Notification")
    print("=" * 80)

    # Create notifier
    if webhook_url:
        notifier = SlackNotifier(webhook_url=webhook_url, enabled=True)
    else:
        notifier = SlackNotifier()

    if not notifier.enabled or not notifier.webhook_url:
        print("❌ Slack not configured, skipping RCA notification test")
        return False

    # Sample RCA analysis (similar to what demo.py produces)
    sample_analysis = {
        "root_cause_service": "cart-service",
        "confidence": 0.85,
        "reasoning": (
            "The cart-service is experiencing database connection timeouts (15% error rate). "
            "Recent PR #156 increased the connection pool size from 10 to 50, but this may have "
            "exposed an underlying database capacity issue. The error logs show 'Database connection "
            "timeout after 5000ms', indicating the database is overwhelmed."
        ),
        "recommended_action": "git revert a3f4b2c && kubectl rollout restart deployment/cart-service"
    }

    print("\n🔄 Sending sample RCA notification...")
    success = notifier.send_rca_analysis(
        analysis=sample_analysis,
        incident_title="Test: Checkout Failures Detected",
        focus_service="cart-service",
        alert_severity="warning"
    )

    if success:
        print("✅ RCA notification sent! Check your Slack channel.")
        return True
    else:
        print("❌ Failed to send RCA notification")
        return False


def main():
    """Main test function."""
    # Check for webhook URL from command line
    webhook_url = None
    if len(sys.argv) > 1:
        webhook_url = sys.argv[1]

    # Run tests
    print("\n🚀 RootScout Slack Integration Test Suite")
    print()

    # Test 1: Basic connection
    basic_ok = test_basic_connection(webhook_url)

    if not basic_ok:
        print("\n" + "=" * 80)
        print("❌ TESTS FAILED")
        print("=" * 80)
        print("\n📖 Setup Instructions:")
        print("   1. Get a Slack webhook URL from https://api.slack.com/messaging/webhooks")
        print("   2. Add it to your .env file:")
        print("      SLACK_WEBHOOK_URL=https://hooks.slack.com/services/YOUR/WEBHOOK/URL")
        print("   3. Set SLACK_ENABLED=true in your .env file")
        print("   4. Run this test again")
        return False

    # Test 2: RCA notification
    rca_ok = test_rca_notification(webhook_url)

    # Summary
    print("\n" + "=" * 80)
    if basic_ok and rca_ok:
        print("✅ ALL TESTS PASSED")
        print("=" * 80)
        print("\n🎉 Slack integration is working correctly!")
        print("   You can now run demo.py to see automatic RCA notifications in Slack.")
        return True
    else:
        print("⚠️  SOME TESTS FAILED")
        print("=" * 80)
        print(f"\n   Basic Connection: {'✅' if basic_ok else '❌'}")
        print(f"   RCA Notification: {'✅' if rca_ok else '❌'}")
        return False


if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\n⚠️  Test interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
