"""A Redis-shaped keyspace held in memory and persisted to Slack, one message per key.

Redis keeps data in RAM and persists it to an append-only file. SlackCache keeps data in RAM and
persists it to a Slack channel. Every write waits for Slack to accept the message, so reads run
at memory speed and writes run at Slack's posting limit of about one message per second.
"""

import binascii
from contextlib import ExitStack
from dataclasses import dataclass
import re
import sys
import threading
import time

from . import codec

FREE_PLAN_HORIZON_MS = 90 * 24 * 3600 * 1000  # The free plan hides messages after 90 days.
REAP_PER_PASS = 20  # Redis's active expiry also checks 20 keys per loop.
INTEGER = re.compile(rb"-?(0|[1-9][0-9]*)")
INT64 = range(-(2**63), 2**63)
SLOTS = 16384  # Redis Cluster's hash slots.


def key_slot(key):
    """Redis Cluster's CRC16 hash slot, honouring {hash tags}."""
    start = key.find(b"{")
    end = key.find(b"}", start + 1)
    if start >= 0 and end > start + 1:
        key = key[start + 1 : end]
    return binascii.crc_hqx(key, 0) % SLOTS


def ts_order(ts):
    seconds, _, fraction = ts.partition(".")
    return int(seconds), int(fraction or 0)


@dataclass(frozen=True)
class Entry:
    ts: str
    value: bytes
    expire_at: int | None
    shard: int = 0

    def deadline(self):
        horizon = ts_order(self.ts)[0] * 1000 + FREE_PLAN_HORIZON_MS
        return horizon if self.expire_at is None else min(self.expire_at, horizon)


class Store:
    """Keys spread over one or more channels by hash slot, like Redis Cluster's shards."""

    def __init__(self, *channels, clock=time.time):
        self._channels = channels
        self._clock = clock
        self._entries = {}
        # One write lock per channel: Slack's posting limit is per channel, so channels write
        # in parallel. Reads never wait for Slack.
        self._locks = [threading.RLock() for _ in channels]
        self.load()

    def _shard(self, key):
        return key_slot(key) * len(self._channels) // SLOTS

    def _lock(self, key):
        return self._locks[self._shard(key)]

    def _all_locks(self):
        stack = ExitStack()
        for lock in self._locks:
            stack.enter_context(lock)
        return stack

    def _now(self):
        return int(self._clock() * 1000)

    def _live(self, key):
        entry = self._entries.get(key)
        return entry if entry is not None and entry.deadline() > self._now() else None

    def _write(self, key, value, expire_at):
        text, blocks = codec.encode(key, value, expire_at)
        shard = self._shard(key)
        old = self._entries.get(key)
        ts = self._channels[shard].post(text, blocks)
        self._entries[key] = Entry(ts, value, expire_at, shard)
        if old is not None:
            self._channels[old.shard].delete(old.ts)

    def _remove(self, key):
        entry = self._entries.pop(key, None)
        if entry is not None:
            self._channels[entry.shard].delete(entry.ts)

    def load(self):
        """Rebuild the keyspace from the channel, the way Redis replays its append-only file."""
        with self._all_locks():
            entries, stale = {}, []
            for shard, channel in enumerate(self._channels):
                for ts, text in channel.messages():
                    record = codec.decode(text)
                    if record is None:
                        continue
                    entry = Entry(ts, record[1], record[2], shard)
                    current = entries.setdefault(record[0], entry)
                    if current is not entry:
                        newer = ts_order(ts) > ts_order(current.ts)
                        stale.append(current if newer else entry)
                        if newer:
                            entries[record[0]] = entry
            self._entries = entries
            for entry in stale:
                self._channels[entry.shard].delete(entry.ts)

    def reap(self):
        """Delete the messages of expired keys, like Redis's active expiry."""
        now = self._now()
        expired = [key for key, entry in list(self._entries.items()) if entry.deadline() <= now]
        for key in expired[:REAP_PER_PASS]:
            with self._lock(key):
                if self._live(key) is None:
                    self._remove(key)

    def maintain(self, stop, sync_every=30.0, tick=1.0):
        """Reap every tick and re-read the channel to pick up other servers' writes."""
        last_sync = self._clock()
        while not stop.wait(tick):
            try:
                self.reap()
                if self._clock() - last_sync >= sync_every:
                    self.load()
                    last_sync = self._clock()
            except Exception as exc:
                print(f"slackcache: background sync failed: {exc}", file=sys.stderr)

    def get(self, key):
        entry = self._live(key)
        return None if entry is None else entry.value

    def set(self, key, value, expire_at=None, keep_ttl=False, nx=False, xx=False):
        with self._lock(key):
            current = self._live(key)
            if (nx and current) or (xx and not current):
                return False
            if keep_ttl and current:
                expire_at = current.expire_at
            self._write(key, value, expire_at)
            return True

    def delete(self, keys):
        removed = 0
        for key in keys:
            with self._lock(key):
                removed += self._live(key) is not None
                self._remove(key)
        return removed

    def exists(self, keys):
        return sum(self._live(key) is not None for key in keys)

    def pttl(self, key):
        entry = self._live(key)
        if entry is None:
            return -2
        return -1 if entry.expire_at is None else max(entry.expire_at - self._now(), 0)

    def expire_at(self, key, at):
        with self._lock(key):
            current = self._live(key)
            if current is None or (at is None and current.expire_at is None):
                return False
            if at is not None and at <= self._now():
                self._remove(key)
            else:
                self._write(key, current.value, at)
            return True

    def incr_by(self, key, amount):
        with self._lock(key):
            current = self._live(key)
            if current is not None and not INTEGER.fullmatch(current.value):
                raise ValueError("value is not an integer or out of range")
            number = (int(current.value) if current else 0) + amount
            if number not in INT64:
                raise ValueError("increment or decrement would overflow")
            self._write(key, str(number).encode(), current.expire_at if current else None)
            return number

    def keys(self):
        now = self._now()
        return [key for key, entry in list(self._entries.items()) if entry.deadline() > now]

    def flush(self):
        with self._all_locks():
            for channel in self._channels:
                for ts, text in list(channel.messages()):
                    if codec.decode(text) is not None:
                        channel.delete(ts)
            self._entries = {}

    def info(self):
        return {
            "slack_channels": ",".join(f"#{channel.name}" for channel in self._channels),
            "cluster_shards": len(self._channels),
            "maxmemory_policy": "allkeys-slack",
            "aof_fsync": "slack",
        }
