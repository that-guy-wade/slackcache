"""The parts of the Slack Web API that SlackCache uses, plus an offline fake."""

import itertools
import json
import time
import urllib.error
import urllib.parse
import urllib.request

BASE = "https://slack.com/api/"


class SlackError(RuntimeError):
    """A Slack API call failed. The message is safe to display (it never holds the token)."""


class SlackAPI:
    def __init__(self, token, http=urllib.request.urlopen, sleep=time.sleep):
        self._token = token
        self._http = http
        self._sleep = sleep

    def call(self, method, **params):
        form = {
            key: json.dumps(value) if isinstance(value, (list, dict, bool)) else str(value)
            for key, value in params.items()
            if value is not None
        }
        data = urllib.parse.urlencode(form).encode("utf-8")
        headers = {"Authorization": f"Bearer {self._token}"}
        while True:
            request = urllib.request.Request(BASE + method, data=data, headers=headers)
            try:
                with self._http(request, timeout=30) as response:
                    result = json.loads(response.read())
            except urllib.error.HTTPError as exc:
                if exc.code != 429:
                    raise SlackError(f"{method} failed with HTTP {exc.code}") from None
                self._sleep(int(exc.headers.get("Retry-After", "1")))
                continue
            except (OSError, json.JSONDecodeError):
                raise SlackError(f"{method} failed") from None
            if not result.get("ok"):
                raise SlackError(f"{method} failed: {result.get('error', 'unknown_error')}")
            return result


class SlackChannel:
    """A public channel that stores one message per key, written by this app's bot."""

    def __init__(self, api, channel="slackcache"):
        self._api = api
        self.bot_id = api.call("auth.test")["bot_id"]
        self.name = channel.lstrip("#")
        self.id = (
            self._find(self.name)
            or api.call("conversations.create", name=self.name)["channel"]["id"]
        )
        api.call("conversations.join", channel=self.id)

    def _find(self, name):
        cursor = None
        while True:
            result = self._api.call(
                "conversations.list",
                types="public_channel",
                exclude_archived=True,
                limit=1000,
                cursor=cursor,
            )
            for channel in result["channels"]:
                if name in (channel["id"], channel["name"]):
                    return channel["id"]
            cursor = result.get("response_metadata", {}).get("next_cursor")
            if not cursor:
                return None

    def post(self, text, blocks):
        result = self._api.call(
            "chat.postMessage",
            channel=self.id,
            text=text,
            blocks=blocks,
            unfurl_links=False,
            unfurl_media=False,
        )
        return result["ts"]

    def delete(self, ts):
        try:
            self._api.call("chat.delete", channel=self.id, ts=ts)
        except SlackError as exc:
            if not str(exc).endswith("message_not_found"):
                raise

    def messages(self):
        """Yield (ts, text) for this bot's messages, newest first."""
        cursor = None
        while True:
            result = self._api.call(
                "conversations.history", channel=self.id, limit=200, cursor=cursor
            )
            for message in result["messages"]:
                if message.get("bot_id") == self.bot_id and not message.get("subtype"):
                    yield message["ts"], message.get("text", "")
            cursor = result.get("response_metadata", {}).get("next_cursor")
            if not cursor:
                return


class FakeChannel:
    """An in-memory channel for --dry-run and tests. Several stores may share one."""

    name = "slackcache-dry-run"
    id = "CDRYRUN"

    def __init__(self, clock=time.time):
        self._clock = clock
        self._counter = itertools.count()
        self.posted = {}

    def post(self, text, blocks):
        ts = f"{self._clock():.0f}.{next(self._counter):06d}"
        self.posted[ts] = text
        return ts

    def delete(self, ts):
        self.posted.pop(ts, None)

    def messages(self):
        yield from sorted(self.posted.items(), key=lambda item: float(item[0]), reverse=True)
