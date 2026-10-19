"""Cache Claude's answers in Slack: twenty questions, a server restart, then twenty cache hits.

Claude is called through the official Anthropic SDK. The cache is an unmodified redis-py client
talking to SlackCache over the Redis protocol, so every answer lands in Slack as a message.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import time

import redis

from slackcache.server import start_in_thread
from slackcache.slack import FakeChannel, SlackAPI, SlackChannel
from slackcache.store import Store

MODEL = "claude-opus-5-5"
PRICE_PER_MTOK = {"input": 4.00, "output": 20.00}
TTL_SECONDS = 7 * 24 * 3600
QUESTIONS = [
    "What is a write-ahead log?",
    "What does fsync guarantee?",
    "What is an LRU cache?",
    "What is cache stampede?",
    "What is a bloom filter?",
    "What is a B-tree?",
    "What is an LSM tree?",
    "What is MVCC?",
    "What is two-phase commit?",
    "What is the CAP theorem?",
    "What is eventual consistency?",
    "What is a vector clock?",
    "What is Raft?",
    "What is a hot partition?",
    "What is read-your-writes consistency?",
    "What is a tombstone in a database?",
    "What is compaction in a log-structured store?",
    "What does TTL mean for a cache entry?",
    "Why do caches need eviction policies?",
    "Is Slack a database?",
]


def ask_claude(question):
    import anthropic

    response = anthropic.Anthropic().beta.messages.create(
        model=MODEL,
        max_tokens=16000,
        output_config={"effort": "low"},
        betas=["server-side-fallback-2026-07-01"],
        fallbacks="default",
        messages=[{"role": "user", "content": f"Answer in one sentence: {question}"}],
    )
    if response.stop_reason == "refusal":
        raise RuntimeError(f"Claude declined: {question}")
    usage = response.usage
    return {
        "answer": "".join(block.text for block in response.content if block.type == "text"),
        "model": response.model,
        "cost_usd": (
            usage.input_tokens * PRICE_PER_MTOK["input"]
            + usage.output_tokens * PRICE_PER_MTOK["output"]
        )
        / 1_000_000,
    }


def ask_offline(question):
    return {"answer": f"(offline) {question}", "model": "offline", "cost_usd": 0.0}


def ask(cache, question, ask_model):
    key = "claude:" + hashlib.sha256(f"{MODEL}\n{question}".encode()).hexdigest()[:16]
    cached = cache.get(key)
    if cached is not None:
        return json.loads(cached), True
    result = {"question": question, **ask_model(question)}
    cache.set(key, json.dumps(result, ensure_ascii=False, indent=1), ex=TTL_SECONDS)
    return result, False


def run(channel, ask_model, label):
    """Start a fresh SlackCache server on the channel, ask every question, then shut it down."""
    port, stop = start_in_thread(Store(channel))
    cache = redis.Redis(port=port)
    started, hits, cost = time.perf_counter(), 0, 0.0
    try:
        for question in QUESTIONS:
            result, hit = ask(cache, question, ask_model)
            hits += hit
            cost += 0 if hit else result["cost_usd"]
    finally:
        cache.close()
        stop()
    seconds = time.perf_counter() - started
    print(f"{label:<22} {hits:>2}/{len(QUESTIONS)} cache hits  {seconds:7.2f}s  ${cost:.4f}")
    return hits, seconds, cost


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--slack",
        metavar="CHANNEL",
        help="use this Slack channel (needs SLACKCACHE_TOKEN); default is an in-memory channel",
    )
    parser.add_argument("--offline", action="store_true", help="skip Claude; canned answers")
    args = parser.parse_args(argv)
    if args.slack:
        channel = SlackChannel(SlackAPI(os.environ["SLACKCACHE_TOKEN"]), args.slack)
    else:
        channel = FakeChannel()
    ask_model = ask_offline if args.offline else ask_claude
    run(channel, ask_model, "run 1 (cold cache)")
    hits, _, cost = run(channel, ask_model, "run 2 (after restart)")
    if hits != len(QUESTIONS) or cost:
        raise SystemExit("expected every answer to come back from Slack after the restart")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
