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


def open_channels(api, names):
    """Find, create and join public channels by name or ID, listing the workspace once."""
    bot_id = api.call("auth.test")["bot_id"]
    known, cursor = {}, None
    while True:
        result = api.call(
            "conversations.list",
            types="public_channel",
            exclude_archived=True,
            limit=1000,
            cursor=cursor,
        )
        for channel in result["channels"]:
            known[channel["name"]] = known[channel["id"]] = channel["id"]
        cursor = result.get("response_metadata", {}).get("next_cursor")
        if not cursor:
            break
    channels = []
    for name in (name.lstrip("#") for name in names):
        channel_id = known.get(name) or api.call("conversations.create", name=name)["channel"]["id"]
        api.call("conversations.join", channel=channel_id)
        channels.append(SlackChannel(api, channel_id, name, bot_id))
    return channels


class SlackChannel:
    """A public channel that stores one message per key, written by this app's bot."""

    def __init__(self, api, channel_id, name, bot_id):
        self._api = api
        self.id = channel_id
        self.name = name
        self.bot_id = bot_id

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

    def __init__(self, name="slackcache-dry-run", clock=time.time):
        self.name = name
        self.id = "C" + name.upper()
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
