import io
import json
import urllib.error
import urllib.parse

import pytest

from slackcache.slack import FakeChannel, SlackAPI, SlackChannel, SlackError


class FakeHTTP:
    """Records requests and replays canned Slack responses by method name."""

    def __init__(self, responses):
        self.responses = responses
        self.requests = []

    def __call__(self, request, timeout):
        method = request.full_url.rsplit("/", 1)[1]
        params = dict(urllib.parse.parse_qsl(request.data.decode()))
        self.requests.append((method, params, request.headers))
        reply = self.responses[method]
        reply = reply.pop(0) if isinstance(reply, list) else reply
        if isinstance(reply, int):
            raise urllib.error.HTTPError(request.full_url, reply, "err", {"Retry-After": "2"}, None)
        return io.BytesIO(json.dumps(reply).encode())


def test_call_sends_token_and_json_params():
    http = FakeHTTP({"chat.postMessage": {"ok": True, "ts": "1.000001"}})
    SlackAPI("xoxb-test", http=http).call("chat.postMessage", blocks=[{"a": 1}], unfurl_links=False)
    method, params, headers = http.requests[0]
    assert headers["Authorization"] == "Bearer xoxb-test"
    assert params == {"blocks": '[{"a": 1}]', "unfurl_links": "false"}


def test_call_waits_out_rate_limits():
    sleeps = []
    http = FakeHTTP({"chat.postMessage": [429, 429, {"ok": True, "ts": "1.1"}]})
    result = SlackAPI("t", http=http, sleep=sleeps.append).call("chat.postMessage")
    assert result["ts"] == "1.1" and sleeps == [2, 2]


def test_errors_never_include_the_token():
    http = FakeHTTP({"chat.postMessage": {"ok": False, "error": "not_in_channel"}})
    with pytest.raises(SlackError, match="not_in_channel") as error:
        SlackAPI("xoxb-secret", http=http).call("chat.postMessage")
    assert "xoxb-secret" not in str(error.value)
    with pytest.raises(SlackError, match="HTTP 500"):
        SlackAPI("t", http=FakeHTTP({"x": 500})).call("x")


def channel_http(history):
    return FakeHTTP(
        {
            "auth.test": {"ok": True, "bot_id": "B1"},
            "conversations.list": [
                {
                    "ok": True,
                    "channels": [{"id": "C0", "name": "general"}],
                    "response_metadata": {"next_cursor": "page2"},
                },
                {"ok": True, "channels": [{"id": "C9", "name": "slackcache"}]},
            ],
            "conversations.join": {"ok": True},
            "conversations.history": history,
            "chat.delete": {"ok": False, "error": "message_not_found"},
        }
    )


def test_channel_finds_joins_and_reads_only_its_own_messages():
    http = channel_http(
        [
            {
                "ok": True,
                "messages": [
                    {"ts": "3.0", "text": "mine", "bot_id": "B1"},
                    {"ts": "2.0", "text": "human", "user": "U1"},
                    {"ts": "1.5", "text": "joined", "bot_id": "B1", "subtype": "channel_join"},
                ],
                "response_metadata": {"next_cursor": "more"},
            },
            {"ok": True, "messages": [{"ts": "1.0", "text": "other bot", "bot_id": "B2"}]},
        ]
    )
    channel = SlackChannel(SlackAPI("t", http=http), "#slackcache")
    assert channel.id == "C9" and channel.bot_id == "B1"
    assert list(channel.messages()) == [("3.0", "mine")]
    assert http.requests[2][1]["cursor"] == "page2"
    channel.delete("3.0")  # already gone is fine


def test_channel_is_created_when_missing():
    http = channel_http([])
    http.responses["conversations.list"] = {"ok": True, "channels": []}
    http.responses["conversations.create"] = {"ok": True, "channel": {"id": "CNEW"}}
    assert SlackChannel(SlackAPI("t", http=http), "cache").id == "CNEW"


def test_fake_channel_orders_newest_first():
    channel = FakeChannel(clock=lambda: 100)
    first, second = channel.post("a", []), channel.post("b", [])
    assert [ts for ts, _ in channel.messages()] == [second, first]
    channel.delete(first)
    assert list(channel.messages()) == [(second, "b")]
