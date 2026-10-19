"""A RESP2 server that speaks enough Redis for real clients, backed by a blocking store."""

import asyncio
import itertools
import random
import re
import threading
import time
from functools import partial

VERSION = "0.1.0"
INT64_MAX = 2**63 - 1
MAX_MULTIBULK = 1024 * 1024
MAX_BULK = 512 * 1024 * 1024
CONFIG = {b"save": b"", b"appendonly": b"yes", b"maxmemory-policy": b"allkeys-slack"}
TX_COMMANDS = {"multi", "exec", "discard", "watch", "unwatch", "quit"}
INTEGER = re.compile(rb"-?[0-9]+")
OK = "OK"


class ProtocolError(Exception):
    pass


class RedisError(Exception):
    """An error reply; the message includes its prefix, e.g. 'ERR syntax error'."""


def wrong_args(name):
    return RedisError(f"ERR wrong number of arguments for '{name}' command")


SYNTAX = "ERR syntax error"
NOT_INTEGER = "ERR value is not an integer or out of range"


class Parser:
    """Incremental RESP2 request parser: feed() bytes, then drain commands()."""

    def __init__(self):
        self.buf = bytearray()

    def feed(self, data):
        self.buf += data

    def commands(self):
        while (argv := self._next()) is not None:
            if argv:
                yield argv

    def _next(self):
        if not self.buf:
            return None
        if self.buf[0] != ord("*"):
            return self._inline()
        end = self.buf.find(b"\r\n")
        if end < 0:
            return None
        count = _header_int(self.buf[1:end], "multibulk length", MAX_MULTIBULK)
        return self._bulks(max(count, 0), end + 2)

    def _inline(self):
        end = self.buf.find(b"\n")
        if end < 0:
            return None
        line = bytes(self.buf[:end]).rstrip(b"\r")
        del self.buf[: end + 1]
        return line.split()

    def _bulks(self, count, pos):
        buf, argv = self.buf, []
        for _ in range(count):
            end = buf.find(b"\r\n", pos)
            if end < 0:
                return None
            if buf[pos] != ord("$"):
                raise ProtocolError(f"expected '$', got '{chr(buf[pos])}'")
            size = _header_int(buf[pos + 1 : end], "bulk length", MAX_BULK)
            if size < 0:
                raise ProtocolError("invalid bulk length")
            start = end + 2
            if len(buf) < start + size + 2:
                return None
            argv.append(bytes(buf[start : start + size]))
            pos = start + size + 2
        del buf[:pos]
        return argv


def _header_int(raw, what, limit):
    if not INTEGER.fullmatch(raw) or int(raw) > limit:
        raise ProtocolError(f"invalid {what}")
    return int(raw)


def encode(value, resp3=False):
    """Encode a reply: str is a status, dict a map (flattened in RESP2), None a null."""
    if isinstance(value, RedisError):
        return b"-" + str(value).replace("\r", " ").replace("\n", " ").encode() + b"\r\n"
    if isinstance(value, str):
        return f"+{value}\r\n".encode()
    if isinstance(value, int):
        return b":%d\r\n" % value
    if isinstance(value, bytes):
        return b"$%d\r\n%s\r\n" % (len(value), value)
    if value is None:
        return (b"$-1\r\n", b"_\r\n")[resp3]
    if isinstance(value, dict):
        return encode_map(value, resp3)
    return b"*%d\r\n" % len(value) + b"".join(encode(item, resp3) for item in value)


def encode_map(value, resp3):
    head = b"%%%d\r\n" % len(value) if resp3 else b"*%d\r\n" % (2 * len(value))
    return head + b"".join(encode(item, resp3) for pair in value.items() for item in pair)


def glob_regex(pattern):
    """Translate a Redis glob (*, ?, [a-z], [^x], backslash escapes) into a bytes regex."""
    out, i = [], 0
    while i < len(pattern):
        ch = pattern[i : i + 1]
        i += 1
        if ch == b"*":
            out.append(b".*")
        elif ch == b"?":
            out.append(b".")
        elif ch == b"[":
            part, i = _bracket(pattern, i)
            out.append(part)
        else:
            if ch == b"\\" and i < len(pattern):
                ch, i = pattern[i : i + 1], i + 1
            out.append(re.escape(ch))
    return re.compile(b"".join(out), re.DOTALL)


def _bracket(pattern, i):
    negate = pattern[i : i + 1] == b"^"
    i += negate
    ranges = []
    while i < len(pattern) and pattern[i] != ord("]"):
        if pattern[i] == ord("\\") and i + 1 < len(pattern):
            i += 1
        if pattern[i + 1 : i + 2] == b"-" and i + 2 < len(pattern):
            ranges.append(sorted((pattern[i], pattern[i + 2])))
            i += 3
        else:
            ranges.append((pattern[i], pattern[i]))
            i += 1
    if not ranges:
        ranges, negate = [(0, 255)], not negate
    body = b"".join(b"\\x%02x-\\x%02x" % (lo, hi) for lo, hi in ranges)
    return b"[" + b"^" * negate + body + b"]", i + 1


def to_int(raw):
    if not INTEGER.fullmatch(raw) or not -INT64_MAX - 1 <= int(raw) <= INT64_MAX:
        raise RedisError(NOT_INTEGER)
    return int(raw)


def now_ms():
    return int(time.time() * 1000)


def absolute_ms(unit, amount):
    """Epoch ms for an EX/PX (relative) or EXAT/PXAT (absolute) amount."""
    at = amount * (1000 if unit in (b"EX", b"EXAT") else 1)
    return at if unit.endswith(b"AT") else at + now_ms()


def expiry_ms(unit, raw, command):
    """Absolute expiry for SET-style options, which must be positive."""
    amount = to_int(raw)
    at = absolute_ms(unit, amount)
    if amount <= 0 or at > INT64_MAX:
        raise RedisError(f"ERR invalid expire time in '{command}' command")
    return at


class Conn:
    _ids = itertools.count(1)

    def __init__(self, store):
        self.store = store
        self.id = next(Conn._ids)
        self.name = None
        self.multi = None
        self.dirty = False
        self.quit = False
        self.resp3 = False


# Connection and admin commands.


def cmd_ping(c, a):
    if len(a) > 1:
        raise wrong_args("ping")
    return a[0] if a else "PONG"


def cmd_hello(c, a):
    if a:
        if not INTEGER.fullmatch(a[0]):
            raise RedisError("ERR Protocol version is not an integer or out of range")
        if int(a[0]) not in (2, 3):
            raise RedisError("NOPROTO unsupported protocol version")
        c.resp3 = int(a[0]) == 3
    return {
        b"server": b"redis",
        b"version": b"7.0.0",
        b"proto": 3 if c.resp3 else 2,
        b"id": c.id,
        b"mode": b"standalone",
        b"role": b"master",
        b"modules": [],
    }


def cmd_quit(c, a):
    c.quit = True
    return OK


def cmd_select(c, a):
    if to_int(a[0]) != 0:
        raise RedisError("ERR DB index is out of range")
    return OK


def cmd_client(c, a):
    sub = a[0].upper()
    if sub == b"SETNAME" and len(a) == 2:
        c.name = a[1]
    elif sub == b"GETNAME":
        return c.name
    elif sub == b"ID":
        return c.id
    return OK


def cmd_command(c, a):
    return 0 if a and a[0].upper() == b"COUNT" else []


def cmd_config(c, a):
    if a[0].upper() != b"GET":
        return OK
    if len(a) < 2:
        raise wrong_args("config|get")
    patterns = [glob_regex(p) for p in a[1:]]
    return {k: v for k, v in CONFIG.items() if any(p.fullmatch(k) for p in patterns)}


def cmd_info(c, a):
    extra = [f"{key}:{value}" for key, value in c.store.info().items()]
    lines = ["# Server", "redis_version:7.0.0", "redis_mode:standalone"]
    lines += [f"slackcache_version:{VERSION}", *extra, "# Keyspace"]
    lines.append(f"db0:keys={len(c.store.keys())},expires=0,avg_ttl=0")
    return "".join(line + "\r\n" for line in lines).encode()


def cmd_flush(c, a):
    c.store.flush()
    return OK


def cmd_time(c, a):
    seconds, micros = divmod(time.time_ns() // 1000, 1_000_000)
    return [str(seconds).encode(), str(micros).encode()]


def cmd_multi(c, a):
    if c.multi is not None:
        raise RedisError("ERR MULTI calls can not be nested")
    c.multi, c.dirty = [], False
    return OK


def cmd_exec(c, a):
    if c.multi is None:
        raise RedisError("ERR EXEC without MULTI")
    queued, dirty = c.multi, c.dirty
    c.multi, c.dirty = None, False
    if dirty:
        raise RedisError("EXECABORT Transaction discarded because of previous errors.")
    return [call(handler, c, args) for handler, args in queued]


def cmd_discard(c, a):
    if c.multi is None:
        raise RedisError("ERR DISCARD without MULTI")
    c.multi, c.dirty = None, False
    return OK


# String and key commands.


def parse_set_options(args):
    opts = {}
    i = 0
    while i < len(args):
        word = args[i].upper()
        if word in (b"NX", b"XX", b"GET", b"KEEPTTL"):
            opts[word] = True
        elif word in (b"EX", b"PX", b"EXAT", b"PXAT") and i + 1 < len(args) and "at" not in opts:
            i += 1
            opts["at"] = (word, args[i])
        else:
            raise RedisError(SYNTAX)
        i += 1
    if {b"NX", b"XX"} <= opts.keys() or {b"KEEPTTL", "at"} <= opts.keys():
        raise RedisError(SYNTAX)
    return opts


FLAG_NAMES = (("keep_ttl", b"KEEPTTL"), ("nx", b"NX"), ("xx", b"XX"))


def cmd_set(c, a):
    key, value, opts = a[0], a[1], parse_set_options(a[2:])
    at = expiry_ms(*opts["at"], "set") if "at" in opts else None
    old = c.store.get(key) if b"GET" in opts else None
    flags = {name: word in opts for name, word in FLAG_NAMES}
    written = c.store.set(key, value, at, **flags)
    if b"GET" in opts:
        return old
    return OK if written else None


def cmd_setex(c, a, unit, command):
    c.store.set(a[0], a[2], expiry_ms(unit, a[1], command))
    return OK


def cmd_setnx(c, a):
    return int(c.store.set(a[0], a[1], nx=True))


def cmd_getset(c, a):
    old = c.store.get(a[0])
    c.store.set(a[0], a[1])
    return old


def cmd_getdel(c, a):
    old = c.store.get(a[0])
    if old is not None:
        c.store.delete([a[0]])
    return old


def cmd_mset(c, a):
    if len(a) % 2:
        raise wrong_args("mset")
    for key, value in zip(a[::2], a[1::2]):
        c.store.set(key, value)
    return OK


def cmd_msetnx(c, a):
    if len(a) % 2:
        raise wrong_args("msetnx")
    if c.store.exists(a[::2]):
        return 0
    cmd_mset(c, a)
    return 1


def cmd_ttl(c, a):
    ms = c.store.pttl(a[0])
    return ms if ms < 0 else (ms + 500) // 1000


def cmd_expire(c, a, unit, command):
    at = absolute_ms(unit, to_int(a[1]))
    if abs(at) > INT64_MAX:
        raise RedisError(f"ERR invalid expire time in '{command}' command")
    if at <= now_ms():
        return c.store.delete([a[0]])
    return int(c.store.expire_at(a[0], at))


def cmd_incr(c, a, sign=1, fixed=None):
    amount = fixed if fixed is not None else to_int(a[1])
    return c.store.incr_by(a[0], sign * amount)


def cmd_append(c, a):
    value = (c.store.get(a[0]) or b"") + a[1]
    c.store.set(a[0], value, keep_ttl=True)
    return len(value)


def cmd_keys(c, a):
    pattern = glob_regex(a[0])
    return [key for key in c.store.keys() if pattern.fullmatch(key)]


def scan_options(args):
    if len(args) % 2:
        raise RedisError(SYNTAX)
    pattern, count = None, 10
    for name, value in zip(args[::2], args[1::2]):
        if name.upper() == b"MATCH":
            pattern = glob_regex(value)
        elif name.upper() == b"COUNT" and to_int(value) > 0:
            count = int(value)
        else:
            raise RedisError(SYNTAX)
    return pattern, count


def cmd_scan(c, a):
    if not INTEGER.fullmatch(a[0]) or int(a[0]) < 0:
        raise RedisError("ERR invalid cursor")
    cursor, (pattern, count) = int(a[0]), scan_options(a[1:])
    keys = sorted(c.store.keys())
    batch = keys[cursor : cursor + count]
    following = cursor + count if cursor + count < len(keys) else 0
    return [str(following).encode(), [k for k in batch if not pattern or pattern.fullmatch(k)]]


def cmd_randomkey(c, a):
    keys = c.store.keys()
    return random.choice(keys) if keys else None


def cmd_type(c, a):
    return "string" if c.store.exists([a[0]]) else "none"


# name -> (handler, arity). Arity counts the command name; negative means "at least".
COMMANDS = {
    "ping": (cmd_ping, -1),
    "echo": (lambda c, a: a[0], 2),
    "hello": (cmd_hello, -1),
    "quit": (cmd_quit, -1),
    "select": (cmd_select, 2),
    "client": (cmd_client, -2),
    "command": (cmd_command, -1),
    "config": (cmd_config, -2),
    "info": (cmd_info, -1),
    "dbsize": (lambda c, a: len(c.store.keys()), 1),
    "flushdb": (cmd_flush, -1),
    "flushall": (cmd_flush, -1),
    "time": (cmd_time, 1),
    "multi": (cmd_multi, 1),
    "exec": (cmd_exec, 1),
    "discard": (cmd_discard, 1),
    "watch": (lambda c, a: OK, -2),
    "unwatch": (lambda c, a: OK, 1),
    "get": (lambda c, a: c.store.get(a[0]), 2),
    "set": (cmd_set, -3),
    "setex": (partial(cmd_setex, unit=b"EX", command="setex"), 4),
    "psetex": (partial(cmd_setex, unit=b"PX", command="psetex"), 4),
    "setnx": (cmd_setnx, 3),
    "getset": (cmd_getset, 3),
    "getdel": (cmd_getdel, 2),
    "mget": (lambda c, a: [c.store.get(key) for key in a], -2),
    "mset": (cmd_mset, -3),
    "msetnx": (cmd_msetnx, -3),
    "del": (lambda c, a: c.store.delete(a), -2),
    "unlink": (lambda c, a: c.store.delete(a), -2),
    "exists": (lambda c, a: c.store.exists(a), -2),
    "type": (cmd_type, 2),
    "ttl": (cmd_ttl, 2),
    "pttl": (lambda c, a: c.store.pttl(a[0]), 2),
    "expire": (partial(cmd_expire, unit=b"EX", command="expire"), 3),
    "pexpire": (partial(cmd_expire, unit=b"PX", command="pexpire"), 3),
    "expireat": (partial(cmd_expire, unit=b"EXAT", command="expireat"), 3),
    "pexpireat": (partial(cmd_expire, unit=b"PXAT", command="pexpireat"), 3),
    "persist": (lambda c, a: int(c.store.expire_at(a[0], None)), 2),
    "incr": (partial(cmd_incr, fixed=1), 2),
    "incrby": (cmd_incr, 3),
    "decr": (partial(cmd_incr, sign=-1, fixed=1), 2),
    "decrby": (partial(cmd_incr, sign=-1), 3),
    "append": (cmd_append, 3),
    "strlen": (lambda c, a: len(c.store.get(a[0]) or b""), 2),
    "keys": (cmd_keys, 2),
    "scan": (cmd_scan, -2),
    "randomkey": (cmd_randomkey, 1),
}


def lookup(argv):
    name = argv[0].decode("latin-1").lower()
    if name not in COMMANDS:
        shown = "".join(f"'{arg[:128].decode('latin-1')}' " for arg in argv[1:])[:128]
        command = argv[0][:128].decode("latin-1")
        raise RedisError(f"ERR unknown command '{command}', with args beginning with: {shown}")
    handler, arity = COMMANDS[name]
    if (arity > 0 and len(argv) != arity) or len(argv) < abs(arity):
        raise wrong_args(name)
    return name, handler


def call(handler, conn, args):
    try:
        return handler(conn, args)
    except RedisError as err:
        return err
    except Exception as err:  # Slack failures and oversized values become ordinary errors.
        return RedisError(f"ERR {err}")


def execute(conn, argv):
    """Run one command for a connection and return its reply value (never raises RedisError)."""
    try:
        name, handler = lookup(argv)
    except RedisError as err:
        if conn.multi is not None:
            conn.dirty = True
        return err
    if conn.multi is not None and name not in TX_COMMANDS:
        conn.multi.append((handler, argv[1:]))
        return "QUEUED"
    return call(handler, conn, argv[1:])


def run_batch(conn, commands):
    replies = []
    for argv in commands:
        if conn.quit:
            break
        replies.append(encode(execute(conn, argv), conn.resp3))
    return b"".join(replies)


async def handle(store, reader, writer):
    conn, parser = Conn(store), Parser()
    try:
        while not conn.quit and (data := await reader.read(65536)):
            parser.feed(data)
            commands, error = [], None
            try:
                commands.extend(parser.commands())
            except ProtocolError as err:
                error = RedisError(f"ERR Protocol error: {err}")
            # Store calls block on Slack HTTP, so a whole pipelined batch runs off the loop.
            writer.write(await asyncio.to_thread(run_batch, conn, commands))
            if error:
                writer.write(encode(error))
                break
            await writer.drain()
    except ConnectionError:
        pass
    finally:
        writer.close()


async def serve(store, host="127.0.0.1", port=6380):
    server = await asyncio.start_server(partial(handle, store), host, port)
    async with server:
        await server.serve_forever()


def start_in_thread(store, host="127.0.0.1", port=0):
    """Serve on a daemon thread with its own event loop; return (bound_port, stop)."""
    ready, state = threading.Event(), {}

    async def main():
        try:
            server = await asyncio.start_server(partial(handle, store), host, port)
            state.update(
                loop=asyncio.get_running_loop(),
                stop=asyncio.Event(),
                port=server.sockets[0].getsockname()[1],
            )
        except OSError as err:
            state["error"] = err
            return
        finally:
            ready.set()
        await state["stop"].wait()
        server.close()

    thread = threading.Thread(target=asyncio.run, args=(main(),), daemon=True)
    thread.start()
    ready.wait()
    if "error" in state:
        raise state["error"]

    def stop():
        state["loop"].call_soon_threadsafe(state["stop"].set)
        thread.join()

    return state["port"], stop
