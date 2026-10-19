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
Any Redis client can now use `redis://127.0.0.1:6380`. Slack does not add you to channels a bot
creates, so find `#slackcache` under **Browse channels**.

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

Reads come from memory, so GETs are fast. Writes wait for Slack.

## Benchmarks

`redis-benchmark` against a free Slack workspace, 2026-10-19 (random keys, `-q`):

| Setup | SET | GET |
| --- | --- | --- |
| 1 channel (`-n 120 -c 8`) | 1.70 requests/s, p50 1,636 ms | 7,716 requests/s, p50 0.96 ms |
| 8 channels (`-n 480 -c 32`) | 18.93 requests/s, p50 983 ms | 6,680 requests/s, p50 1.08 ms |

GET is about 4,500 times faster than SET. With 8 channels, writes ran at about 30 per second
for the first 400 messages, then Slack throttled the app to about 4 per second.

## Cluster mode

Slack's posting limit is per channel, so SlackCache scales writes the way Redis Cluster does:
more shards. `--shards 8` spreads keys over `#slackcache-1` … `#slackcache-8` using Redis
Cluster's own hash slots (CRC16 mod 16384, with `{hash tags}`). Channels write in parallel:
8 channels measured 11 times the write throughput of one, until Slack's app-wide throttle.
Clients still connect to one endpoint.

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

Live on a free workspace (2026-10-19, one channel):

| Run | Cache hits | Time | Claude cost |
| --- | --- | --- | --- |
| Cold cache | 0/300 | 254.5 s | $0.80 |
| After restarting SlackCache from Slack (300 keys loaded in 0.59 s) | 300/300 | 0.20 s | $0.00 |

Asked to explain a database "to someone who thinks Slack is a database", Claude answered:
"Slack keeps your messages, but it's like a giant group chat where finding something means
scrolling or hoping search guesses right." It is now cached in Slack.

Use `--offline` to run it without Claude, or omit `--slack` to use in-memory channels.

## Supported commands

Strings and keys: `GET SET (EX PX EXAT PXAT NX XX KEEPTTL GET) SETEX PSETEX SETNX GETSET GETDEL
MGET MSET MSETNX DEL UNLINK EXISTS TYPE TTL PTTL EXPIRE PEXPIRE EXPIREAT PEXPIREAT PERSIST INCR
INCRBY DECR DECRBY APPEND STRLEN KEYS SCAN RANDOMKEY`. Server: `PING ECHO SELECT 0 INFO DBSIZE
FLUSHDB FLUSHALL TIME CONFIG CLIENT COMMAND MULTI EXEC DISCARD WATCH`. No lists, hashes or sets.

## Limits

- One key and value must fit in one Slack message: about 11.9 KB of value. Slack's docs give
  40,000 characters for message text, and say that with blocks the limit depends on the block
  type. In our tests, text sent with blocks was cut to exactly 16,000 characters, and text sent
  without blocks was split into separate 4,000-character messages. Every write checks that Slack
  stored exactly what was sent.
- `chat.postMessage` allows about one message per second per channel, with short bursts.
  Deletes and history reads are Tier 3 (about 50 per minute). `FLUSHALL` on a large keyspace
  takes minutes.
- An app installed only in your own workspace keeps Tier 3 history reads. Slack's 2025 limit
  of one history read per minute applies to commercially distributed apps, not to this one.
- Free workspaces allow 10 app installations.
- A throttled write can take several seconds, so give your Redis client a socket timeout of a
  minute or more.
- MULTI/EXEC runs commands in order but is not isolated from other servers. INCR is atomic
  within one server only.

References: [Slack free plan limits](https://slack.com/help/articles/29414264463635),
[rate limits](https://docs.slack.dev/apis/web-api/rate-limits),
[chat.postMessage](https://docs.slack.dev/reference/methods/chat.postMessage).

## Project

Part of the "is it a database?" series, after
[GranolaDB](https://github.com/that-guy-wade/granoladb) and
[CalendarDB](https://github.com/that-guy-wade/calendardb).
