from crawl.plan_a.utils import normalize_url


def test_normalize_url_strips_utm_and_orders_params():
    url = "https://EXAMPLE.com/Path?a=1&utm_source=x&b=2&utm_campaign=y"
    norm = normalize_url(url)
    assert norm == "https://example.com/Path?a=1&b=2"


def test_normalize_url_handles_ports_and_trailing():
    url = "http://example.com:80/index.html?"
    norm = normalize_url(url)
    assert norm == "http://example.com/index.html"

