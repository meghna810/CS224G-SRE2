"""
Slack Notifier Client
=====================
Sends formatted RCA analysis results to Slack using webhook URLs.
"""

import os
import json
import requests
from typing import Dict, Any, Optional
from datetime import datetime


class SlackNotifier:
    """
    Client for sending RCA analysis results to Slack.

    Usage:
        # Initialize with webhook URL from environment
        notifier = SlackNotifier()

        # Or pass webhook URL directly
        notifier = SlackNotifier(webhook_url="https://hooks.slack.com/services/...")

        # Send RCA analysis
        notifier.send_rca_analysis(
            analysis={"root_cause_service": "cart-service", ...},
            incident_title="Checkout failures detected",
            focus_service="cart-service"
        )
    """

    def __init__(self, webhook_url: Optional[str] = None, enabled: Optional[bool] = None):
        """
        Initialize Slack notifier.

        Args:
            webhook_url: Slack webhook URL. If not provided, reads from SLACK_WEBHOOK_URL env var.
            enabled: Whether to enable Slack notifications. If not provided, reads from SLACK_ENABLED env var.
        """
        self.webhook_url = webhook_url or os.getenv("SLACK_WEBHOOK_URL")

        # Check if enabled (defaults to True if webhook URL is set)
        if enabled is not None:
            self.enabled = enabled
        else:
            env_enabled = os.getenv("SLACK_ENABLED", "true").lower()
            self.enabled = env_enabled in ("true", "1", "yes")

        if not self.webhook_url and self.enabled:
            print("⚠️  Slack notifications enabled but SLACK_WEBHOOK_URL not set in .env")
            self.enabled = False

    def send_rca_analysis(
        self,
        analysis: Dict[str, Any],
        incident_title: str = "RCA Analysis Complete",
        focus_service: Optional[str] = None,
        alert_severity: str = "warning"
    ) -> bool:
        """
        Send RCA analysis results to Slack with rich formatting.

        Args:
            analysis: RCA analysis dictionary from agent.analyze()
            incident_title: Title for the incident
            focus_service: The service that triggered the alert
            alert_severity: Severity level (critical, warning, info)

        Returns:
            True if message was sent successfully, False otherwise
        """
        if not self.enabled:
            print("ℹ️  Slack notifications disabled, skipping...")
            return False

        if not self.webhook_url:
            print("❌ Cannot send to Slack: webhook URL not configured")
            return False

        # Extract analysis fields
        root_cause = analysis.get("root_cause_service", "Unknown")
        confidence = analysis.get("confidence", 0.0)
        reasoning = analysis.get("reasoning", "No reasoning provided")
        action = analysis.get("recommended_action", "No action recommended")

        # Determine emoji based on severity and confidence
        if confidence >= 0.8:
            confidence_emoji = "🎯"
        elif confidence >= 0.6:
            confidence_emoji = "🤔"
        else:
            confidence_emoji = "⚠️"

        severity_emoji = {
            "critical": "🔴",
            "warning": "🟡",
            "info": "🔵"
        }.get(alert_severity.lower(), "⚪")

        # Build Slack message with rich formatting
        message = self._build_slack_message(
            incident_title=incident_title,
            focus_service=focus_service or root_cause,
            root_cause=root_cause,
            confidence=confidence,
            reasoning=reasoning,
            action=action,
            severity_emoji=severity_emoji,
            confidence_emoji=confidence_emoji
        )

        try:
            response = requests.post(
                self.webhook_url,
                json=message,
                headers={"Content-Type": "application/json"},
                timeout=10
            )

            if response.status_code == 200:
                print("✅ Slack notification sent successfully")
                return True
            else:
                print(f"❌ Failed to send Slack notification: {response.status_code} - {response.text}")
                return False

        except Exception as e:
            print(f"❌ Error sending Slack notification: {e}")
            return False

    def _build_slack_message(
        self,
        incident_title: str,
        focus_service: str,
        root_cause: str,
        confidence: float,
        reasoning: str,
        action: str,
        severity_emoji: str,
        confidence_emoji: str
    ) -> Dict[str, Any]:
        """
        Build Slack Block Kit message with rich formatting.

        Returns:
            Slack message payload
        """
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        confidence_pct = int(confidence * 100)

        # Build blocks for rich message
        blocks = [
            # Header
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"{severity_emoji} {incident_title}",
                    "emoji": True
                }
            },
            # Divider
            {"type": "divider"},
            # Summary section
            {
                "type": "section",
                "fields": [
                    {
                        "type": "mrkdwn",
                        "text": f"*Focus Service:*\n`{focus_service}`"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Root Cause:*\n`{root_cause}`"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Confidence:*\n{confidence_emoji} {confidence_pct}%"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Timestamp:*\n{timestamp}"
                    }
                ]
            },
            # Divider
            {"type": "divider"},
            # Analysis section
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*💡 Analysis*\n{reasoning}"
                }
            },
            # Divider
            {"type": "divider"},
            # Recommended action section
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*🔧 Recommended Action*\n```{action}```"
                }
            },
            # Footer
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": "🤖 Generated by RootScout AI | Powered by Gemini"
                    }
                ]
            }
        ]

        return {
            "blocks": blocks,
            # Fallback text for notifications
            "text": f"{incident_title} - Root cause: {root_cause} ({confidence_pct}% confidence)"
        }

    def send_simple_message(self, text: str) -> bool:
        """
        Send a simple text message to Slack.

        Args:
            text: Message text

        Returns:
            True if sent successfully, False otherwise
        """
        if not self.enabled or not self.webhook_url:
            return False

        try:
            response = requests.post(
                self.webhook_url,
                json={"text": text},
                headers={"Content-Type": "application/json"},
                timeout=10
            )
            return response.status_code == 200
        except Exception:
            return False

    def test_connection(self) -> bool:
        """
        Test Slack webhook connection.

        Returns:
            True if connection successful, False otherwise
        """
        if not self.webhook_url:
            print("❌ No webhook URL configured")
            return False

        return self.send_simple_message("✅ RootScout Slack integration test successful!")
