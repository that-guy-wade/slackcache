import binascii
import threading
import time

import pytest

from slackcache import codec
from slackcache.slack import FakeChannel
from slackcache.store import FREE_PLAN_HORIZON_MS, REAP_PER_PASS, Store, key_slot


class Clock:
    def __init__(self, now=1_760_000_000.0):
        self.now = now

    def __call__(self):
        return self.now


@pytest.fixture
def clock():
    return Clock()


@pytest.fixture
def channel(clock):
    return FakeChannel(clock=clock)


@pytest.fixture
def store(channel, clock):
    return Store(channel, clock=clock)


def keys_in_slack(channel):
    return sorted(codec.decode(text)[0] for _, text in channel.messages())


def test_values_persist_in_slack_and_survive_a_restart(store, channel, clock):
    store.set(b"a", b"1")
    store.set(b"b", b"2", expire_at=int(clock() * 1000) + 60_000)
    store.set(b"a", b"3")
    assert len(channel.posted) == 2 and keys_in_slack(channel) == [b"a", b"b"]
    restarted = Store(channel, clock=clock)
    assert restarted.get(b"a") == b"3" and restarted.get(b"b") == b"2"
    assert 0 < restarted.pttl(b"b") <= 60_000 and restarted.pttl(b"a") == -1


def test_second_server_sees_writes_after_sync(store, channel, clock):
    other = Store(channel, clock=clock)
    store.set(b"k", b"v")
    assert other.get(b"k") is None
    other.load()
    assert other.get(b"k") == b"v"


def test_load_keeps_newest_duplicate_and_deletes_the_rest(channel, clock):
    old = channel.post(codec.encode(b"k", b"old")[0], [])
    clock.now += 1
    channel.post(codec.encode(b"k", b"new")[0], [])
    channel.post("someone chatting", [])
    store = Store(channel, clock=clock)
    assert store.get(b"k") == b"new"
    assert old not in channel.posted and len(channel.posted) == 2


def test_nx_xx_keepttl(store, clock):
    later = int(clock() * 1000) + 5_000
    assert store.set(b"k", b"1", nx=True, expire_at=later)
    assert not store.set(b"k", b"2", nx=True)
    assert not store.set(b"missing", b"2", xx=True)
    assert store.set(b"k", b"3", xx=True, keep_ttl=True)
    assert store.get(b"k") == b"3" and store.pttl(b"k") == 5_000


def test_expired_keys_vanish_and_are_reaped_from_slack(store, channel, clock):
    store.set(b"k", b"v", expire_at=int(clock() * 1000) + 1_000)
    clock.now += 1
    assert store.get(b"k") is None and store.exists([b"k"]) == 0 and store.pttl(b"k") == -2
    assert store.keys() == [] and len(channel.posted) == 1
    store.reap()
    assert channel.posted == {}


def test_reaper_deletes_a_bounded_batch(store, channel, clock):
    for i in range(REAP_PER_PASS + 5):
        store.set(b"%d" % i, b"v", expire_at=int(clock() * 1000) + 1)
    clock.now += 1
    store.reap()
    assert len(channel.posted) == 5


def test_free_plan_evicts_keys_after_90_days(store, clock):
    store.set(b"forever", b"v")
    clock.now += FREE_PLAN_HORIZON_MS / 1000 - 1
    assert store.get(b"forever") == b"v" and store.pttl(b"forever") == -1
    clock.now += 1
    assert store.get(b"forever") is None


def test_rewriting_a_key_resets_the_free_plan_clock(store, clock):
    store.set(b"k", b"v")
    clock.now += FREE_PLAN_HORIZON_MS / 1000 - 1
    store.set(b"k", b"v")
    clock.now += 10
    assert store.get(b"k") == b"v"


def test_delete_exists_and_duplicates(store, channel):
    store.set(b"a", b"1")
    store.set(b"b", b"2")
    assert store.exists([b"a", b"a", b"c"]) == 2
    assert store.delete([b"a", b"a", b"c"]) == 1
    assert keys_in_slack(channel) == [b"b"]


def test_expire_and_persist(store, channel, clock):
    now = int(clock() * 1000)
    store.set(b"k", b"v")
    assert not store.expire_at(b"k", None)
    assert store.expire_at(b"k", now + 10_000) and store.pttl(b"k") == 10_000
    assert store.expire_at(b"k", None) and store.pttl(b"k") == -1
    assert codec.decode(next(channel.messages())[1])[2] is None
    assert store.expire_at(b"k", now) and store.get(b"k") is None and channel.posted == {}
    assert not store.expire_at(b"missing", now + 1)


def test_incr_by(store, clock):
    store.set(b"n", b"41", expire_at=int(clock() * 1000) + 9_000)
    assert store.incr_by(b"n", 1) == 42 and store.pttl(b"n") == 9_000
    assert store.incr_by(b"new", -5) == -5
    for bad in (b"4.2", b" 1", b"1_0", b"01", b""):
        store.set(b"bad", bad)
        with pytest.raises(ValueError, match="not an integer"):
            store.incr_by(b"bad", 1)
    store.set(b"max", b"%d" % (2**63 - 1))
    with pytest.raises(ValueError, match="overflow"):
        store.incr_by(b"max", 1)


def test_flush_deletes_every_slackcache_message_but_not_chat(store, channel):
    store.set(b"a", b"1")
    channel.post(codec.encode(b"from-another-server", b"x")[0], [])
    channel.post("lunch?", [])
    store.flush()
    assert store.keys() == [] and list(channel.posted.values()) == ["lunch?"]


def test_too_large_values_are_rejected_without_writing(store, channel):
    with pytest.raises(codec.ValueTooLarge):
        store.set(b"k", b"x" * 12_000)
    assert channel.posted == {} and store.get(b"k") is None


def test_failed_slack_write_leaves_the_keyspace_unchanged(store, channel):
    store.set(b"k", b"old")

    def down(*args):
        raise RuntimeError("slack is down")

    channel.post = down
    with pytest.raises(RuntimeError):
        store.set(b"k", b"new")
    assert store.get(b"k") == b"old"


def test_maintain_reaps_and_syncs_until_stopped(store, channel, clock):
    other = Store(channel, clock=clock)
    store.set(b"k", b"v")
    stop = threading.Event()
    kwargs = {"tick": 0.01, "sync_every": 0}
    thread = threading.Thread(target=other.maintain, args=(stop,), kwargs=kwargs)
    thread.start()
    for _ in range(200):
        if other.get(b"k"):
            break
        stop.wait(0.01)
    stop.set()
    thread.join(1)
    assert other.get(b"k") == b"v" and not thread.is_alive()


def test_key_slot_matches_redis_cluster():
    assert key_slot(b"foo") == 12182 and key_slot(b"bar") == 5061
    assert key_slot(b"{user1000}.following") == key_slot(b"{user1000}.followers")
    assert key_slot(b"foo{}{bar}") == binascii.crc_hqx(b"foo{}{bar}", 0) % 16384  # empty tag


def test_cluster_spreads_keys_and_survives_a_restart(clock):
    channels = [FakeChannel(f"cache-{i}", clock=clock) for i in range(4)]
    store = Store(*channels, clock=clock)
    for i in range(200):
        store.set(b"key:%d" % i, b"%d" % i)
    counts = [len(channel.posted) for channel in channels]
    assert sum(counts) == 200 and min(counts) > 30
    store.delete([b"key:0"])
    restarted = Store(*channels, clock=clock)
    assert len(restarted.keys()) == 199 and restarted.get(b"key:199") == b"199"
    assert restarted.info()["slack_channels"] == "#cache-0,#cache-1,#cache-2,#cache-3"


def test_resharding_moves_keys_on_their_next_write(clock):
    one = FakeChannel("one", clock=clock)
    Store(one, clock=clock).set(b"k", b"old")
    two = FakeChannel("two", clock=clock)
    store = Store(one, two, clock=clock)
    assert store.get(b"k") == b"old"
    store.set(b"k", b"new")
    assert sum(len(channel.posted) for channel in (one, two)) == 1
    assert Store(one, two, clock=clock).get(b"k") == b"new"


def test_shards_write_in_parallel(clock):
    class SlowChannel(FakeChannel):
        def post(self, text, blocks):
            time.sleep(0.2)
            return super().post(text, blocks)

    store = Store(*(SlowChannel(f"s{i}", clock=clock) for i in range(4)), clock=clock)
    keys = [b"a", b"b", b"c", b"d"]
    assert len({store._shard(key) for key in keys}) == 4
    started = time.perf_counter()
    threads = [threading.Thread(target=store.set, args=(key, b"v")) for key in keys]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert time.perf_counter() - started < 0.6
