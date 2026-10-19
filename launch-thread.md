1/7 Is it a database? SlackCache is a NEW, entirely FREE Redis-compatible cache.

Every key is a Slack message. Free Slack hides messages after 90 days anyway, so we called it an eviction policy.

[Screenshot: #claude-cache scrolled to a run of cached answers]

2/7 It speaks the Redis protocol, so redis-cli, redis-benchmark and redis-py connect unchanged.

GET: 7,716 requests/sec
SET: 1.70 requests/sec

Reads come from RAM. Writes wait for Slack.

[Screenshot: redis-benchmark output]

3/7 Real workload: we cached 300 Claude answers in one channel.

Cold: 254 seconds, $0.80.
Then we restarted SlackCache. It reloaded every key from Slack in 0.59 seconds.
Asked again: 300/300 cache hits, 0.2 seconds, $0.

4/7 Need more write throughput? Cluster mode shards keys across channels using Redis Cluster's own hash slots.

1 channel: 1.70 SETs/sec
8 channels: 18.9 SETs/sec

11x faster, until Slack throttled us.

5/7 Things we learned the hard way: Slack quietly cut our messages at 16,000 characters, so a value can be up to 11.9 KB. Every write now checks that Slack stored exactly what we sent.

INFO reports maxmemory_policy:allkeys-slack.

6/7 We asked Claude to explain a database to someone who thinks Slack is a database:

"Slack keeps your messages, but it's like a giant group chat where finding something means scrolling or hoping search guesses right."

That answer is now cached in Slack.

[Screenshot: that message in #claude-cache]

7/7 SlackCache is satire and not affiliated with Slack. It runs on your own workspace with an app you create from a manifest.

[TODO before posting: repo link once public, and `pip install slackcache` once published to PyPI]

Previously: GranolaDB and CalendarDB.
