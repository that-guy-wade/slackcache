"""Command-line entry point: a Redis-compatible server whose storage is a Slack channel."""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import threading

from .server import serve
from .slack import FakeChannel, SlackAPI, SlackChannel, SlackError
from .store import Store

TOKEN_ENV = "SLACKCACHE_TOKEN"


def _channel(args):
    if args.dry_run:
        return FakeChannel()
    token = os.environ.get(TOKEN_ENV)
    if not token:
        raise SlackError(f"set {TOKEN_ENV} to your Slack app's bot token (xoxb-...)")
    return SlackChannel(SlackAPI(token), args.channel)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="slackcache")
    commands = parser.add_subparsers(dest="command", required=True)
    serve_parser = commands.add_parser("serve", help="Run a Redis-compatible server on Slack")
    serve_parser.add_argument("--channel", default="slackcache", help="public channel name or ID")
    serve_parser.add_argument("--host", default="127.0.0.1")
    serve_parser.add_argument("--port", type=int, default=6380)
    serve_parser.add_argument(
        "--sync-every",
        type=float,
        default=30.0,
        metavar="SECONDS",
        help="re-read the channel to pick up other servers' writes",
    )
    serve_parser.add_argument(
        "--dry-run", action="store_true", help="keep keys in memory instead of Slack"
    )
    args = parser.parse_args(argv)
    try:
        channel = _channel(args)
        store = Store(channel)
    except SlackError as error:
        print(f"slackcache: {error}", file=sys.stderr)
        return 1
    stop = threading.Event()
    threading.Thread(
        target=store.maintain, args=(stop,), kwargs={"sync_every": args.sync_every}, daemon=True
    ).start()
    print(
        f"SlackCache ready on redis://{args.host}:{args.port} "
        f"(#{channel.name}, {len(store.keys())} keys loaded)",
        flush=True,
    )
    try:
        asyncio.run(serve(store, args.host, args.port))
    except KeyboardInterrupt:
        pass
    finally:
        stop.set()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
