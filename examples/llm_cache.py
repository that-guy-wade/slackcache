"""Cache Claude's answers in Slack: hundreds of questions, a server restart, then all cache hits.

Claude is called through the official Anthropic SDK. The cache is an unmodified redis-py client
talking to SlackCache over the Redis protocol, so every answer lands in Slack as a message.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import itertools
import json
import os
import time

import redis

from slackcache.server import start_in_thread
from slackcache.slack import FakeChannel, SlackAPI, open_channels
from slackcache.store import Store

MODEL = "claude-opus-5-5"
PRICE_PER_MTOK = {"input": 4.00, "output": 20.00}
TTL_SECONDS = 7 * 24 * 3600
TERMS = [
    "a write-ahead log", "fsync", "an LRU cache", "a cache stampede", "a bloom filter",
    "a B-tree", "an LSM tree", "MVCC", "two-phase commit", "the CAP theorem",
    "eventual consistency", "a vector clock", "Raft", "a hot partition", "a tombstone",
    "compaction", "a TTL", "an eviction policy", "sharding", "replication lag",
    "a primary key", "an index", "a deadlock", "a query planner", "a database",
]  # fmt: skip
AUDIENCES = [
    "a five-year-old", "a pirate", "a medieval knight", "a cat", "a sommelier",
    "a sports commentator", "a haiku poet", "a startup founder pitching investors",
    "a tired on-call engineer at 3am", "a Shakespearean actor", "a golden retriever",
    "someone who thinks Slack is a database",
]  # fmt: skip
QUESTIONS = [f"Explain {term} to {who}." for term, who in itertools.product(TERMS, AUDIENCES)]


def ask_claude(question):
    import anthropic

    response = anthropic.Anthropic().beta.messages.create(
        model=MODEL,
        max_tokens=16000,
        output_config={"effort": "low"},
        betas=["server-side-fallback-2026-07-01"],
        fallbacks="default",
        messages=[{"role": "user", "content": f"Answer in one or two sentences. {question}"}],
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
    """Return (result, cache_hit) for one question, caching misses in SlackCache."""
    key = "claude:" + hashlib.sha256(f"{MODEL}\n{question}".encode()).hexdigest()[:16]
    cached = cache.get(key)
    if cached is not None:
        return json.loads(cached), True
    result = {"question": question, **ask_model(question)}
    cache.set(key, json.dumps(result, ensure_ascii=False, indent=1), ex=TTL_SECONDS)
    return result, False


def run(channels, questions, ask_model, workers, label):
    """Start a fresh SlackCache server on the channels, ask every question, then shut it down."""
    port, stop = start_in_thread(Store(*channels))
    cache = redis.Redis(port=port, max_connections=workers)
    started = time.perf_counter()
    try:
        with ThreadPoolExecutor(workers) as pool:
            results = list(pool.map(lambda q: ask(cache, q, ask_model), questions))
    finally:
        cache.close()
        stop()
    seconds = time.perf_counter() - started
    hits = sum(hit for _, hit in results)
    cost = sum(result["cost_usd"] for result, hit in results if not hit)
    print(
        f"{label:<22} {hits:>4}/{len(questions)} cache hits  {seconds:8.2f}s  ${cost:.4f}",
        flush=True,
    )
    return hits, cost


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--slack",
        metavar="CHANNEL",
        help="store answers in this Slack "
        "channel (needs SLACKCACHE_TOKEN); default is an in-memory channel",
    )
    parser.add_argument("--shards", type=int, default=1, help="spread keys over N channels")
    parser.add_argument("--questions", type=int, default=len(QUESTIONS))
    parser.add_argument("--workers", type=int, default=8, help="concurrent questions")
    parser.add_argument("--offline", action="store_true", help="skip Claude; canned answers")
    args = parser.parse_args(argv)
    name = args.slack or "slackcache-demo"
    names = [name] if args.shards == 1 else [f"{name}-{i}" for i in range(1, args.shards + 1)]
    if args.slack:
        channels = open_channels(SlackAPI(os.environ["SLACKCACHE_TOKEN"]), names)
    else:
        channels = [FakeChannel(n) for n in names]
    questions = QUESTIONS[: args.questions]
    ask_model = ask_offline if args.offline else ask_claude
    run(channels, questions, ask_model, args.workers, "run 1 (cold cache)")
    hits, cost = run(channels, questions, ask_model, args.workers, "run 2 (after restart)")
    if hits != len(questions) or cost:
        raise SystemExit("expected every answer to come back from Slack after the restart")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
