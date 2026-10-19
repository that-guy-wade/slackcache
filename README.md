# SlackCache

> A Redis-compatible cache stored in a free Slack workspace. Eviction policy: the free plan.

Free Slack workspaces hide messages after 90 days and, since August 2024, delete them after a
year. We were already losing our data, so we called it a cache.

SlackCache speaks the Redis protocol. `redis-cli`, `redis-benchmark` and ordinary Redis client
libraries connect to it unchanged. Every key is a Slack message in a channel you own: the
message shows the key, its expiry and a preview of the value, and its text holds the exact bytes.

> [!NOTE]
> **Satire.** SlackCache is a joke, not affiliated with Slack, and not for production or
> sensitive data. It runs on your own workspace with an app you create, and posts only to its
> own channel.

## Install

```sh
pip install slackcache
slackcache serve --dry-run          # in-memory channel, no Slack needed
redis-cli -p 6380 SET hello world EX 60
redis-cli -p 6380 GET hello
```

## Connect it to Slack

1. Open https://api.slack.com/apps, choose **Create New App → From a manifest**, pick your
   workspace and paste [`slack-app-manifest.yml`](slack-app-manifest.yml).
2. **Install to Workspace**, then copy the **Bot User OAuth Token** (`xoxb-…`).
3. Run the server:

```sh
export SLACKCACHE_TOKEN=xoxb-...
slackcache serve --channel slackcache
```

SlackCache creates `#slackcache` if it is missing, joins it and loads every key already there.
Any Redis client can now use `redis://127.0.0.1:6380`.

## How it works

Redis keeps data in RAM and persists it to an append-only file. SlackCache keeps data in RAM and
persists it to a Slack channel:

| Redis | SlackCache |
| --- | --- |
| Append-only file | `#slackcache` |
| `appendfsync always` | Every write waits for Slack to accept the message |
| Replaying the AOF on startup | Reading the channel history on startup |
| Active expiry (20 keys per cycle) | Deleting 20 expired keys' messages per second |
| `maxmemory-policy allkeys-lru` | `allkeys-slack`: the free plan hides anything not rewritten in 90 days |
| Replicas | Every server reading the same channel; they re-sync every 30 seconds |

Writes post a new message and delete the key's previous one, so the channel always holds one
message per live key, newest at the bottom. If two servers write the same key, the message with
the later Slack timestamp wins and the older one is deleted.

Reads come from memory, so GETs are fast. Writes run at Slack's posting limit of about one
message per second per channel.

## Cluster mode

Slack's posting limit is per channel, so SlackCache scales writes the way Redis Cluster does:
more shards. `--shards 8` spreads keys over `#slackcache-1` … `#slackcache-8` using Redis
Cluster's own hash slots (CRC16 mod 16384, with `{hash tags}`). Each channel writes in
parallel, adding roughly one write per second. Clients still connect to one endpoint.

```sh
slackcache serve --shards 8
```

Changing the shard count is safe. Keys stay readable in their old channel and move to their new
one the next time they are written.

## Demo: cache Claude's answers in Slack

[`examples/llm_cache.py`](examples/llm_cache.py) asks Claude 300 questions ("Explain an LSM
tree to a pirate", "Explain the CAP theorem to a golden retriever", …) through the official
Anthropic SDK, eight at a time, caching each answer through an unmodified redis-py client. Each
answer becomes a readable Slack message. It then restarts SlackCache from the channels alone and
asks again:

```sh
pip install anthropic
export ANTHROPIC_API_KEY=... SLACKCACHE_TOKEN=xoxb-...
python examples/llm_cache.py --slack slackcache-demo --shards 8
```

With an in-memory channel, `claude-opus-5-5` answered all 300 in 99 s for $0.80. After the
restart, all 300 came back from the cache in 0.28 s for $0. Use `--offline` to run it without
Claude, or omit `--slack` to use in-memory channels.

## Supported commands

Strings and keys: `GET SET (EX PX EXAT PXAT NX XX KEEPTTL GET) SETEX PSETEX SETNX GETSET GETDEL
MGET MSET MSETNX DEL UNLINK EXISTS TYPE TTL PTTL EXPIRE PEXPIRE EXPIREAT PEXPIREAT PERSIST INCR
INCRBY DECR DECRBY APPEND STRLEN KEYS SCAN RANDOMKEY`. Server: `PING ECHO SELECT 0 INFO DBSIZE
FLUSHDB FLUSHALL TIME CONFIG CLIENT COMMAND MULTI EXEC DISCARD WATCH`. No lists, hashes or sets.

## Limits

- One key and value must fit in one Slack message: about 29 KB of value.
- `chat.postMessage` allows about one message per second per channel, with short bursts.
  Deletes and history reads are Tier 3 (about 50 per minute). `FLUSHALL` on a large keyspace
  takes minutes.
- An app installed only in your own workspace keeps Tier 3 history reads. Slack's 2025 limit
  of one history read per minute applies to commercially distributed apps, not to this one.
- Free workspaces allow 10 app installations.
- MULTI/EXEC runs commands in order but is not isolated from other servers. INCR is atomic
  within one server only.

References: [Slack free plan limits](https://slack.com/help/articles/29414264463635),
[rate limits](https://docs.slack.dev/apis/web-api/rate-limits),
[chat.postMessage](https://docs.slack.dev/reference/methods/chat.postMessage).

## Project

Part of the "is it a database?" series, after
[GranolaDB](https://github.com/that-guy-wade/granoladb) and
[CalendarDB](https://github.com/that-guy-wade/calendardb).
