"""The real Store behind the real server, driven by unmodified redis-py."""

import redis
import pytest

from slackcache import codec
from slackcache.server import start_in_thread
from slackcache.slack import FakeChannel
from slackcache.store import Store


@pytest.fixture
def channel():
    return FakeChannel()


def serve(channel):
    port, stop = start_in_thread(Store(channel))
    return redis.Redis(port=port), stop


def test_keys_survive_a_server_restart(channel):
    client, stop = serve(channel)
    client.set("user:42", "Ada", ex=60)
    client.mset({"a": 1, "b": 2})
    client.incr("visits")
    stop()
    client, stop = serve(channel)
    try:
        assert client.get("user:42") == b"Ada" and 0 < client.ttl("user:42") <= 60
        assert client.mget("a", "b", "visits") == [b"1", b"2", b"1"]
        assert sorted(codec.decode(text)[0] for _, text in channel.messages()) == [
            b"a",
            b"b",
            b"user:42",
            b"visits",
        ]
    finally:
        stop()


def test_store_errors_become_redis_errors(channel):
    client, stop = serve(channel)

    def slack_down(*args):
        raise RuntimeError("chat.postMessage failed: ratelimited")

    try:
        with pytest.raises(redis.ResponseError, match="too large for a Slack message"):
            client.set("big", b"x" * 40_000)
        client.set("n", "not a number")
        with pytest.raises(redis.ResponseError, match="not an integer"):
            client.incr("n")
        channel.post = slack_down
        with pytest.raises(redis.ResponseError, match="ratelimited"):
            client.set("k", "v")
        assert client.ping() and client.get("k") is None
    finally:
        stop()


def test_pipelines_and_transactions(channel):
    client, stop = serve(channel)
    try:
        with client.pipeline(transaction=True) as pipe:
            pipe.set("x", 1).incrby("x", 4).get("x")
            assert pipe.execute() == [True, 5, b"5"]
        assert client.dbsize() == 1 and client.info()["maxmemory_policy"] == "allkeys-slack"
    finally:
        stop()
