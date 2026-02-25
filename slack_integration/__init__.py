"""
Slack Integration Module
========================
Sends RCA analysis results to Slack channels automatically.
"""

from .client import SlackNotifier

__all__ = ["SlackNotifier"]
