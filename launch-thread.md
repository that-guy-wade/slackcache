1/3 Is it a database? SlackCache is a NEW, entirely FREE Redis-compatible cache.

Every key is a Slack message. Slack's free plan was already deleting our data, so we turned it into a cache eviction policy.

GET: 7,716 requests/sec
SET: 1.70 requests/sec

[Screenshot: #claude-cache]

2/3 We cached 300 Claude answers in it. Cold: 254s, $0.80. After a restart: 300/300 hits, 0.2s, $0.

Claude, explaining databases to someone who thinks Slack is one: "it's like a giant group chat where finding something means scrolling or hoping search guesses right."

[Screenshot: that message in #claude-cache]

3/3 It speaks the Redis protocol, so redis-cli and redis-py work unchanged. Satire, not affiliated with Slack.

[TODO before posting: repo link once public]
