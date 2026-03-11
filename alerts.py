import os
import socket
from abc import ABC, abstractmethod
from enum import Enum
from typing import List

import requests


class _Severity(Enum):
    INFO = "info"
    SUCCESS = "success"
    WARN = "warn"
    ERROR = "error"


class AlertBackend(ABC):
    @abstractmethod
    def send(self, message: str, severity: _Severity) -> None: ...


class DiscordBackend(AlertBackend):
    """Sends alerts via Discord webhook. Set DISCORD_WEBHOOK_URL env var.

    Uses ANSI color codes: green for success, red for error, blue for info.
    """

    _ANSI_COLORS = {
        _Severity.INFO:    "\u001b[34m",  # blue
        _Severity.SUCCESS: "\u001b[32m",  # green
        _Severity.WARN:    "\u001b[33m",  # yellow
        _Severity.ERROR:   "\u001b[31m",  # red
    }
    _ANSI_RESET = "\u001b[0m"

    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url

    def send(self, message: str, severity: _Severity) -> None:
        color = self._ANSI_COLORS[severity]
        formatted = f"```ansi\n{color}{message}{self._ANSI_RESET}\n```"
        requests.post(self.webhook_url, json={
            "username": socket.gethostname(),
            "content": formatted,
        })


class SlackBackend(AlertBackend):
    """Sends alerts via Slack incoming webhook. Set SLACK_WEBHOOK_URL env var.

    Uses colored attachment sidebar: green for success, red for error, blue for info.
    """

    _COLORS = {
        _Severity.INFO:    "#2196F3",
        _Severity.SUCCESS: "#4CAF50",
        _Severity.WARN:    "#FF9800",
        _Severity.ERROR:   "#F44336",
    }

    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url

    def send(self, message: str, severity: _Severity) -> None:
        requests.post(self.webhook_url, json={
            "attachments": [{
                "color": self._COLORS[severity],
                "text": f"[{socket.gethostname()}] {message}",
            }],
        })


class TelegramBackend(AlertBackend):
    """Sends alerts via Telegram Bot API. Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID env vars.

    Uses HTML formatting with emoji prefixes for severity.
    """

    _PREFIXES = {
        _Severity.INFO:    "\u2139\ufe0f",
        _Severity.SUCCESS: "\u2705",
        _Severity.WARN:    "\u26a0\ufe0f",
        _Severity.ERROR:   "\u274c",
    }

    def __init__(self, bot_token: str, chat_id: str):
        self.bot_token = bot_token
        self.chat_id = chat_id

    def send(self, message: str, severity: _Severity) -> None:
        prefix = self._PREFIXES[severity]
        requests.post(
            f"https://api.telegram.org/bot{self.bot_token}/sendMessage",
            json={
                "chat_id": self.chat_id,
                "parse_mode": "HTML",
                "text": f"{prefix} <b>[{socket.gethostname()}]</b> {message}",
            },
        )


class Alerting:
    """Dispatches messages to all configured alert backends.

    Backends are auto-detected from environment variables:
      - DISCORD_WEBHOOK_URL  -> Discord
      - SLACK_WEBHOOK_URL    -> Slack
      - TELEGRAM_BOT_TOKEN + TELEGRAM_CHAT_ID -> Telegram
    """

    def __init__(self, backends: List[AlertBackend] | None = None):
        if backends is not None:
            self.backends = backends
        else:
            self.backends = _backends_from_env()

    def info(self, message: str) -> None:
        for backend in self.backends:
            backend.send(message, _Severity.INFO)

    def success(self, message: str) -> None:
        for backend in self.backends:
            backend.send(message, _Severity.SUCCESS)

    def warn(self, message: str) -> None:
        for backend in self.backends:
            backend.send(message, _Severity.WARN)

    def error(self, message: str) -> None:
        for backend in self.backends:
            backend.send(message, _Severity.ERROR)


def _backends_from_env() -> List[AlertBackend]:
    backends: List[AlertBackend] = []

    if url := os.getenv("DISCORD_WEBHOOK_URL"):
        backends.append(DiscordBackend(url))

    
    if url := os.getenv("SLACK_WEBHOOK_URL"):
        backends.append(SlackBackend(url))

    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if token and chat_id:
        backends.append(TelegramBackend(token, chat_id))

    return backends
