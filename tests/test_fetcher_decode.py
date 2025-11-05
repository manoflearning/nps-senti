from crawl.core.fetch.fetcher import Fetcher
import requests


def test_decode_bytes_uses_meta_charset_cp949():
    # HTML declares euc-kr; bytes are cp949-compatible
    sample_text = "보배드림 제목"
    html = (
        '<html><head><meta http-equiv="Content-Type" content="text/html; charset=euc-kr"></head>'
        f"<body><h1>{sample_text}</h1></body></html>"
    )
    data = html.encode("cp949")
    f = Fetcher(requests.Session(), timeout=3)
    text, enc = f._decode_bytes(data, content_type=None, apparent=None)
    assert sample_text in text
    assert enc in {"euc-kr", "cp949"}


def test_decode_bytes_uses_apparent_encoding():
    sample_text = "한글 본문"
    data = f"<html><body>{sample_text}</body></html>".encode("cp949")
    f = Fetcher(requests.Session(), timeout=3)
    text, enc = f._decode_bytes(data, content_type=None, apparent="cp949")
    assert sample_text in text
    assert enc == "cp949"
