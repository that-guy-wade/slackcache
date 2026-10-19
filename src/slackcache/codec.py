"""One Slack message per key: a machine-readable `text` and a human-readable preview."""

import base64
from datetime import datetime, timezone

MARKER = "slackcache:v1"
MAX_TEXT = 39_000  # Slack truncates message text past 40,000 characters.
PREVIEW = 1_500


class ValueTooLarge(ValueError):
    """The key and value do not fit in one Slack message."""


def _b64(data):
    return base64.b64encode(data).decode("ascii")


def _show(data, limit):
    text = data.decode("utf-8", errors="replace")
    if len(text) <= limit:
        return text or "(empty string)"
    return f"{text[:limit]}… ({len(data):,} bytes)"


def _expiry_note(expire_at):
    if expire_at is None:
        return " · no TTL (the free plan evicts it in 90 days)"
    at = datetime.fromtimestamp(expire_at / 1000, timezone.utc)
    return f" · expires {at:%Y-%m-%d %H:%M:%S} UTC"


def encode(key, value, expire_at=None):
    """Return (text, blocks). `text` is canonical; `blocks` is what people see in Slack."""
    text = f"{MARKER} {_b64(key)} {'-' if expire_at is None else expire_at} {_b64(value)}"
    if len(text) > MAX_TEXT:
        raise ValueTooLarge("value too large for a Slack message")
    header = [
        {"type": "text", "text": "SET ", "style": {"bold": True}},
        {"type": "text", "text": _show(key, 200), "style": {"code": True}},
        {"type": "text", "text": _expiry_note(expire_at)},
    ]
    body = [{"type": "text", "text": _show(value, PREVIEW)}]
    blocks = [
        {
            "type": "rich_text",
            "elements": [
                {"type": "rich_text_section", "elements": header},
                {"type": "rich_text_preformatted", "elements": body},
            ],
        }
    ]
    return text, blocks


def decode(text):
    """Return (key, value, expire_at) from a message's text, or None if it is not ours."""
    parts = (text or "").split(" ")
    if len(parts) != 4 or parts[0] != MARKER:
        return None
    try:
        key, value = (base64.b64decode(part, validate=True) for part in (parts[1], parts[3]))
        expire_at = None if parts[2] == "-" else int(parts[2])
    except ValueError:
        return None
    return key, value, expire_at
