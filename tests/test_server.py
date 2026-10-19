import shutil
import socket
import subprocess
import threading
import time

import pytest
import redis

from slackcache.server import Parser, ProtocolError, encode, glob_regex, start_in_thread


def now_ms():
    return int(time.time() * 1000)


class DictStore:
    """Thread-safe in-memory stand-in for the Slack-backed Store interface."""

    def __init__(self, delay=0.0):
        self.data = {}  # key -> [value, expire_at | None]
        self.lock = threading.Lock()
        self.delay = delay

    def _live(self, key):
        if self.delay:
            time.sleep(self.delay)
        entry = self.data.get(key)
        if entry and entry[1] is not None and entry[1] <= now_ms():
            del self.data[key]
            return None
        return entry

    def get(self, key):
        with self.lock:
            entry = self._live(key)
            return entry[0] if entry else None

    def set(self, key, value, expire_at=None, keep_ttl=False, nx=False, xx=False):
        with self.lock:
            entry = self._live(key)
            if (nx and entry) or (xx and not entry):
                return False
            if keep_ttl and entry:
                expire_at = entry[1]
            self.data[key] = [value, expire_at]
            return True

    def delete(self, keys):
        with self.lock:
            return sum(self._live(k) is not None and self.data.pop(k) is not None for k in keys)

    def exists(self, keys):
        with self.lock:
            return sum(self._live(k) is not None for k in keys)

    def pttl(self, key):
        with self.lock:
            entry = self._live(key)
            if not entry:
                return -2
            return -1 if entry[1] is None else max(entry[1] - now_ms(), 0)

    def expire_at(self, key, at):
        with self.lock:
            entry = self._live(key)
            if not entry or (at is None and entry[1] is None):
                return False
            entry[1] = at
            return True

    def incr_by(self, key, amount):
        with self.lock:
            entry = self._live(key) or [b"0", None]
            try:
                value = int(entry[0]) + amount
            except ValueError:
                value = None
            if value is None or not -(2**63) <= value < 2**63:
                raise ValueError("value is not an integer or out of range")
            self.data[key] = [str(value).encode(), entry[1]]
            return value

    def keys(self):
        with self.lock:
            return [k for k in list(self.data) if self._live(k)]

    def flush(self):
        with self.lock:
            self.data.clear()

    def info(self):
        return {"slack_channel": "#cache"}


@pytest.fixture(scope="module")
def server():
    store = DictStore()
    port, stop = start_in_thread(store)
    yield port, store
    stop()


@pytest.fixture(params=[None, 2], ids=["default-resp3", "resp2"])
def r(server, request):
    client = redis.Redis(port=server[0], protocol=request.param)
    client.flushdb()
    yield client
    client.close()


def raw(port, payload, replies=1):
    """Send raw bytes and return what the server answers (stops after `replies` CRLF lines)."""
    with socket.create_connection(("127.0.0.1", port), timeout=5) as sock:
        sock.sendall(payload)
        data = b""
        while data.count(b"\r\n") < replies:
            chunk = sock.recv(65536)
            if not chunk:
                break
            data += chunk
        return data


# --- RESP parser ---


def parse(*chunks):
    parser, out = Parser(), []
    for chunk in chunks:
        parser.feed(chunk)
        out.extend(parser.commands())
    return out


def test_parser_byte_by_byte():
    payload = b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\nhello\r\n"
    assert parse(*(payload[i : i + 1] for i in range(len(payload)))) == [[b"SET", b"k", b"hello"]]


def test_parser_partial_returns_nothing_until_complete():
    parser = Parser()
    parser.feed(b"*2\r\n$3\r\nGET\r\n$3\r\nab")
    assert list(parser.commands()) == []
    parser.feed(b"c")
    assert list(parser.commands()) == []
    parser.feed(b"\r\n")
    assert list(parser.commands()) == [[b"GET", b"abc"]]
    assert parser.buf == b""


def test_parser_binary_bulk_with_crlf_inside():
    value = b"a\r\nb\x00\xff\r\n"
    payload = b"*2\r\n$4\r\nECHO\r\n$%d\r\n%s\r\n" % (len(value), value)
    assert parse(payload[:12], payload[12:]) == [[b"ECHO", value]]


def test_parser_pipelined_and_inline():
    payload = b"*1\r\n$4\r\nPING\r\nSET  k   v\r\n\r\nGET k\n*0\r\n*1\r\n$4\r\nPING\r\n"
    assert parse(payload) == [[b"PING"], [b"SET", b"k", b"v"], [b"GET", b"k"], [b"PING"]]


@pytest.mark.parametrize("payload", [b"*x\r\n", b"*1\r\n:3\r\n", b"*1\r\n$-5\r\n"])
def test_parser_protocol_errors(payload):
    with pytest.raises(ProtocolError):
        parse(payload)


def test_encode():
    assert encode([b"a", None, 3, "OK", []]) == b"*5\r\n$1\r\na\r\n$-1\r\n:3\r\n+OK\r\n*0\r\n"
    assert encode({b"k": None}) == b"*2\r\n$1\r\nk\r\n$-1\r\n"
    assert encode({b"k": None}, resp3=True) == b"%1\r\n$1\r\nk\r\n_\r\n"


@pytest.mark.parametrize(
    "pattern,key,match",
    [
        (b"*", b"anything", True),
        (b"h?llo", b"hello", True),
        (b"h?llo", b"hllo", False),
        (b"h*llo", b"heeeello", True),
        (b"h[ae]llo", b"hallo", True),
        (b"h[ae]llo", b"hillo", False),
        (b"h[^e]llo", b"hallo", True),
        (b"h[^e]llo", b"hello", False),
        (b"h[a-b]llo", b"hbllo", True),
        (b"h[b-a]llo", b"hbllo", True),
        (b"h\\*llo", b"h*llo", True),
        (b"h\\*llo", b"hello", False),
        (b"a.b", b"axb", False),
        (b"user:[0-9]*", b"user:7:x", True),
        (b"[]", b"x", False),
        (b"*\n*", b"a\nb", True),
    ],
)
def test_glob(pattern, key, match):
    assert bool(glob_regex(pattern).fullmatch(key)) is match


# --- Exact wire replies ---


def test_error_wording(server):
    port = server[0]
    assert raw(port, b"FOO a b\r\n") == (
        b"-ERR unknown command 'FOO', with args beginning with: 'a' 'b' \r\n"
    )
    assert raw(port, b"PING x\r\n") == b"$1\r\nx\r\n"
    assert raw(port, b"PING a b\r\n") == b"-ERR wrong number of arguments for 'ping' command\r\n"
    assert raw(port, b"COMMAND DOCS\r\n") == b"*0\r\n"
    assert raw(port, b"GET\r\n") == b"-ERR wrong number of arguments for 'get' command\r\n"
    assert raw(port, b"SET k v EX 0\r\n") == b"-ERR invalid expire time in 'set' command\r\n"
    assert raw(port, b"SET k v NX XX\r\n") == b"-ERR syntax error\r\n"
    assert raw(port, b"*1\r\n$x\r\n") == b"-ERR Protocol error: invalid bulk length\r\n"


def test_hello_switches_protocol(server):
    reply = raw(server[0], b"HELLO 3\r\nGET missing\r\nCONFIG GET save\r\n", replies=19)
    assert reply.startswith(b"%7\r\n$6\r\nserver\r\n$5\r\nredis\r\n")
    assert b"$5\r\nproto\r\n:3\r\n" in reply
    assert reply.endswith(b"_\r\n%1\r\n$4\r\nsave\r\n$0\r\n\r\n")
    reply = raw(server[0], b"HELLO 2\r\nGET missing\r\n", replies=16)
    assert reply.startswith(b"*14\r\n") and reply.endswith(b"$-1\r\n")
    assert raw(server[0], b"HELLO 4\r\n") == b"-NOPROTO unsupported protocol version\r\n"


def test_pipelined_packet_replies_in_order(server):
    payload = b"SET p 1\r\nINCR p\r\nINCRBY p 5\r\nGET p\r\nPING\r\nQUIT\r\nPING\r\n"
    assert raw(server[0], payload, replies=6) == b"+OK\r\n:2\r\n:7\r\n$1\r\n7\r\n+PONG\r\n+OK\r\n"


def test_quit_closes_connection(server):
    with socket.create_connection(("127.0.0.1", server[0]), timeout=5) as sock:
        sock.sendall(b"QUIT\r\n")
        assert sock.recv(100) == b"+OK\r\n"
        assert sock.recv(100) == b""


# --- redis-py integration ---


def test_connection_commands(r):
    assert r.ping() is True
    assert r.echo(b"hi") == b"hi"
    assert r.execute_command("SELECT", 0) is True
    with pytest.raises(redis.ResponseError, match="DB index is out of range"):
        r.execute_command("SELECT", 1)
    assert r.client_setname("me") is True
    assert r.client_getname() == "me"
    assert isinstance(r.client_id(), int)
    assert r.command_count() == 0
    assert r.command() == {}
    assert r.config_get("*") == {
        "save": "",
        "appendonly": "yes",
        "maxmemory-policy": "allkeys-slack",
    }
    assert r.config_get("nope") == {}
    assert r.config_set("maxmemory", "1") is True
    seconds, micros = r.time()
    assert abs(seconds - time.time()) < 5 and 0 <= micros < 1_000_000


def test_info_dbsize_flush(r):
    r.mset({"a": 1, "b": 2})
    info = r.info()
    assert info["redis_version"] == "7.0.0"
    assert info["slack_channel"] == "#cache"
    assert info["db0"]["keys"] == 2
    assert r.dbsize() == 2
    assert r.flushall() is True
    assert r.dbsize() == 0


def test_get_set_and_options(r):
    assert r.get("k") is None
    assert r.set("k", "v") is True
    assert r.get("k") == b"v"
    assert r.set("k", "w", nx=True) is None
    assert r.set("new", "x", xx=True) is None
    assert r.set("k", "w", xx=True) is True
    assert r.set("k", "z", get=True) == b"w"
    assert r.set("fresh", "z", get=True) is None
    assert r.set("k", "y", nx=True, get=True) == b"z"
    assert r.get("k") == b"z"


@pytest.mark.parametrize(
    "args,error",
    [
        (["NX", "XX"], "syntax error"),
        (["EX", "1", "PX", "1"], "syntax error"),
        (["EX", "10", "KEEPTTL"], "syntax error"),
        (["EX"], "syntax error"),
        (["BOGUS"], "syntax error"),
        (["EX", "abc"], "value is not an integer or out of range"),
        (["PX", "-1"], "invalid expire time in 'set' command"),
        (["EXAT", "0"], "invalid expire time in 'set' command"),
    ],
)
def test_set_option_errors(r, args, error):
    with pytest.raises(redis.ResponseError, match=error):
        r.execute_command("SET", "k", "v", *args)
    assert r.exists("k") == 0


def test_set_expiry_variants(r):
    r.set("ex", "v", ex=100)
    assert 99 <= r.ttl("ex") <= 100
    r.set("px", "v", px=100_000)
    assert 99_000 < r.pttl("px") <= 100_000
    r.set("exat", "v", exat=int(time.time()) + 50)
    assert 48 <= r.ttl("exat") <= 50
    r.set("pxat", "v", pxat=now_ms() + 50_000)
    assert 49 <= r.ttl("pxat") <= 50
    r.set("pxat", "w", keepttl=True)
    assert r.get("pxat") == b"w" and r.ttl("pxat") > 0
    r.set("pxat", "x")
    assert r.ttl("pxat") == -1


def test_key_expires(r):
    r.set("short", "v", px=50)
    assert 0 < r.pttl("short") <= 50
    time.sleep(0.12)
    assert r.get("short") is None
    assert r.ttl("short") == -2
    assert r.exists("short") == 0


def test_setex_family(r):
    assert r.execute_command("SETEX", "a", 100, "v") is True and 99 <= r.ttl("a") <= 100
    assert r.psetex("b", 100_000, "v") is True and r.pttl("b") > 99_000
    with pytest.raises(redis.ResponseError, match="invalid expire time in 'setex' command"):
        r.execute_command("SETEX", "a", 0, "v")
    with pytest.raises(redis.ResponseError, match="invalid expire time in 'psetex' command"):
        r.execute_command("PSETEX", "b", -5, "v")
    assert r.setnx("a", "x") is False
    assert r.setnx("c", "x") is True
    assert r.getset("c", "y") == b"x" and r.get("c") == b"y"
    assert r.getdel("c") == b"y" and r.exists("c") == 0
    assert r.getdel("c") is None


def test_multi_key_commands(r):
    assert r.mset({"a": "1", "b": "2"}) is True
    assert r.mget("a", "missing", "b") == [b"1", None, b"2"]
    assert r.msetnx({"b": "x", "c": "3"}) is False
    assert r.exists("c") == 0
    assert r.msetnx({"c": "3", "d": "4"}) is True
    assert r.exists("a", "a", "missing", "c") == 3
    assert r.delete("a", "missing") == 1
    assert r.unlink("b", "c") == 2
    with pytest.raises(redis.ResponseError, match="wrong number of arguments for 'mset'"):
        r.execute_command("MSET", "a", "1", "b")


def test_type_ttl_expire_persist(r, server):
    r.set("k", "v")
    assert r.type("k") == b"string"
    assert r.type("nope") == b"none"
    assert r.ttl("k") == -1 and r.ttl("nope") == -2
    assert r.expire("k", 100) is True and 99 <= r.ttl("k") <= 100
    assert r.pexpire("k", 50_000) is True and 49_000 < r.pttl("k") <= 50_000
    assert r.expireat("k", int(time.time()) + 200) is True and 198 <= r.ttl("k") <= 200
    assert r.pexpireat("k", now_ms() + 300_000) is True and r.ttl("k") > 298
    assert r.persist("k") is True and r.ttl("k") == -1
    assert r.persist("k") is False
    assert r.expire("nope", 10) is False
    assert r.expire("k", 0) is True and b"k" not in server[1].data
    r.set("k", "v")
    assert r.expireat("k", 1) is True and b"k" not in server[1].data
    assert r.pexpire("nope", -1) is False


def test_ttl_rounds_like_redis(r):
    r.set("k", "v", px=1600)
    assert r.ttl("k") == 2


def test_counters(r):
    assert r.incr("n") == 1
    assert r.incrby("n", 10) == 11
    assert r.decr("n") == 10
    assert r.decrby("n", 4) == 6
    assert r.get("n") == b"6"
    r.set("s", "abc")
    with pytest.raises(redis.ResponseError, match="value is not an integer or out of range"):
        r.incr("s")
    with pytest.raises(redis.ResponseError, match="value is not an integer or out of range"):
        r.incrby("n", "1.5")
    r.set("big", str(2**63 - 1))
    with pytest.raises(redis.ResponseError, match="value is not an integer or out of range"):
        r.incr("big")


def test_append_strlen_keep_ttl(r):
    assert r.append("k", "ab") == 2
    r.expire("k", 100)
    assert r.append("k", "cd") == 4
    assert r.get("k") == b"abcd" and r.ttl("k") > 0
    assert r.strlen("k") == 4 and r.strlen("nope") == 0


def test_keys_scan_randomkey(r):
    assert r.randomkey() is None
    names = [f"user:{i}".encode() for i in range(25)] + [b"other", b"h[x]"]
    r.mset({name: b"v" for name in names})
    assert sorted(r.keys("user:1*")) == sorted([b"user:1"] + [b"user:1%d" % i for i in range(10)])
    assert r.keys("h\\[x\\]") == [b"h[x]"]
    assert sorted(r.keys()) == sorted(names)
    assert sorted(r.scan_iter(count=4)) == sorted(names)
    assert sorted(r.scan_iter(match="user:2?", count=3)) == [b"user:2%d" % i for i in range(5)]
    cursor, first = r.scan(0, count=5)
    assert cursor == 5 and len(first) == 5
    assert r.randomkey() in names
    with pytest.raises(redis.ResponseError, match="invalid cursor"):
        r.execute_command("SCAN", "x")
    with pytest.raises(redis.ResponseError, match="syntax error"):
        r.execute_command("SCAN", 0, "COUNT", 0)


def test_binary_safe(r):
    key, value = bytes(range(256)) + b"\r\n", b"\x00\r\n" * 1000 + bytes(range(256))
    r.set(key, value)
    assert r.get(key) == value
    assert r.keys("*") == [key]


@pytest.mark.parametrize("transaction", [True, False])
def test_pipeline(r, transaction):
    pipe = r.pipeline(transaction=transaction)
    pipe.set("a", "1").incr("a").get("a").mget("a", "missing").delete("a")
    assert pipe.execute() == [True, 2, b"2", [b"2", None], 1]


def test_multi_exec_errors(r, server):
    pipe = r.pipeline(transaction=True)
    pipe.set("s", "x").incr("s").get("s")
    with pytest.raises(redis.ResponseError, match="value is not an integer"):
        pipe.execute()
    assert r.get("s") == b"x"
    assert raw(server[0], b"EXEC\r\n") == b"-ERR EXEC without MULTI\r\n"
    assert raw(server[0], b"DISCARD\r\n") == b"-ERR DISCARD without MULTI\r\n"
    reply = raw(server[0], b"MULTI\r\nSET q 1\r\nNOPE\r\nEXEC\r\nGET q\r\n", replies=5)
    assert reply.startswith(b"+OK\r\n+QUEUED\r\n-ERR unknown command 'NOPE'")
    assert reply.endswith(
        b"-EXECABORT Transaction discarded because of previous errors.\r\n$-1\r\n"
    )
    assert raw(server[0], b"MULTI\r\nSET q 1\r\nDISCARD\r\nGET q\r\n", 4) == (
        b"+OK\r\n+QUEUED\r\n+OK\r\n$-1\r\n"
    )


def test_watch_pipeline(r):
    with r.pipeline() as pipe:
        pipe.watch("w")
        pipe.multi()
        pipe.set("w", "1")
        assert pipe.execute() == [True]


def test_slow_store_does_not_block_other_clients():
    store = DictStore()
    port, stop = start_in_thread(store)
    try:
        store.delay = 0.5
        slow = threading.Thread(target=redis.Redis(port=port).get, args=("k",))
        slow.start()
        time.sleep(0.05)
        started = time.monotonic()
        assert redis.Redis(port=port).ping() is True
        assert time.monotonic() - started < 0.3
        slow.join()
    finally:
        stop()


def test_pipeline_order_with_slow_store():
    store = DictStore(delay=0.002)
    port, stop = start_in_thread(store)
    try:
        pipe = redis.Redis(port=port).pipeline(transaction=False)
        for i in range(100):
            pipe.set("k", i).get("k")
        assert pipe.execute()[1::2] == [str(i).encode() for i in range(100)]
    finally:
        stop()


# --- Real Redis tools ---


cli = pytest.mark.skipif(not shutil.which("redis-cli"), reason="redis-cli not installed")
bench = pytest.mark.skipif(not shutil.which("redis-benchmark"), reason="redis-benchmark missing")


def run(*args):
    return subprocess.run(args, capture_output=True, text=True, timeout=60)


@cli
def test_redis_cli_round_trip(server):
    port = str(server[0])
    assert run("redis-cli", "-p", port, "SET", "cli", "v a l").stdout.strip() == "OK"
    assert run("redis-cli", "-p", port, "GET", "cli").stdout.strip() == "v a l"
    info = run("redis-cli", "-p", port, "--no-raw", "INFO")
    assert info.returncode == 0 and "redis_version:7.0.0" in info.stdout
    assert "slack_channel:#cache" in info.stdout


@bench
@pytest.mark.parametrize("pipeline", ["1", "16"])
def test_redis_benchmark(server, pipeline):
    result = run(
        "redis-benchmark", "-p", str(server[0]), "-t", "set,get", "-n", "200", "-q", "-P", pipeline
    )
    assert result.returncode == 0, result.stderr
    assert "SET: " in result.stdout and "GET: " in result.stdout
    assert "WARN" not in result.stdout + result.stderr
