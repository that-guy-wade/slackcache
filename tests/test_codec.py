import pytest

from slackcache import codec


@pytest.mark.parametrize(
    "key, value, expire_at",
    [
        (b"user:42", b'{"name": "Ada"}', None),
        (b"", b"", 1_760_000_000_000),
        (b"bin\r\n\x00", bytes(range(256)), 1),
        ("ключ <&>".encode(), "https://example.com :tada: <!here>".encode(), None),
    ],
)
def test_round_trip(key, value, expire_at):
    text, blocks = codec.encode(key, value, expire_at)
    assert codec.decode(text) == (key, value, expire_at)
    assert blocks[0]["type"] == "rich_text"


def test_text_survives_slack_formatting():
    text, _ = codec.encode(b"<@U123>", b"https://evil.example/?a=b&c=:smile:")
    assert all(ch.isalnum() or ch in " +/=:-" for ch in text)


def test_preview_is_readable_and_bounded():
    _, blocks = codec.encode(b"page", b"x" * 10_000, 1_760_000_000_000)
    section, preformatted = blocks[0]["elements"]
    assert section["elements"][1]["text"] == "page"
    assert "expires 2025-10-09 08:53:20 UTC" in section["elements"][2]["text"]
    assert preformatted["elements"][0]["text"].endswith("… (10,000 bytes)")


def test_rejects_values_slack_would_truncate():
    with pytest.raises(codec.ValueTooLarge):
        codec.encode(b"k", b"x" * 30_000)


@pytest.mark.parametrize(
    "text",
    ["", "hello", "slackcache:v1 a b", "slackcache:v1 !! - AA==", "slackcache:v1 AA== soon AA=="],
)
def test_ignores_other_messages(text):
    assert codec.decode(text) is None
