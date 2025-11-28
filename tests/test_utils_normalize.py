from crawl.core.utils import normalize_url


def test_normalize_url_strips_utm_and_orders_params():
    url = "https://EXAMPLE.com/Path?a=1&utm_source=x&b=2&utm_campaign=y"
    norm = normalize_url(url)
    assert norm == "https://example.com/Path?a=1&b=2"


def test_normalize_url_handles_ports_and_trailing():
    url = "http://example.com:80/index.html?"
    norm = normalize_url(url)
    assert norm == "http://example.com/index.html"


def test_normalize_bobaedream_view_keeps_code_and_no_only():
    url = "https://www.bobaedream.co.kr/view?code=freeb&No=3336459&bm=1&cmt=1&utm_source=x"
    norm = normalize_url(url)
    assert norm == "https://www.bobaedream.co.kr/view?No=3336459&code=freeb"


def test_normalize_mlbpark_keeps_board_and_id():
    url = "https://mlbpark.donga.com/mp/b.php?b=bullpen&m=view&id=202511030110552085&utm_source=x"
    norm = normalize_url(url)
    # order of params is sorted
    assert norm == (
        "https://mlbpark.donga.com/mp/b.php?b=bullpen&id=202511030110552085"
    )


def test_normalize_kmib_strips_sid_and_stg():
    url_with_sid = (
        "https://news.kmib.co.kr/article/view.asp?"
        "arcid=0924244207&code=11151100&sid1=eco"
    )
    url_with_stg = (
        "https://news.kmib.co.kr/article/view.asp?"
        "arcid=0924244207&code=11151100&stg=wm_rank"
    )

    norm_sid = normalize_url(url_with_sid)
    norm_stg = normalize_url(url_with_stg)

    expected = "https://news.kmib.co.kr/article/view.asp?arcid=0924244207&code=11151100"

    assert norm_sid == expected
    assert norm_stg == expected


def test_normalize_moneys_keeps_no():
    url = "http://moneys.mt.co.kr/news/mwView.php?no=2019103114188092934&code=&MGSPN"
    norm = normalize_url(url)
    assert norm == "http://moneys.mt.co.kr/news/mwView.php?no=2019103114188092934"


def test_normalize_moneytoday_keeps_no():
    url = (
        "https://news.mt.co.kr/hotview.php?"
        "no=2024101408290989806&type=1&sec=all&hid=202410021402187110&hcnt=83"
    )
    norm = normalize_url(url)
    assert norm == "https://news.mt.co.kr/hotview.php?no=2024101408290989806"


def test_normalize_koreaherald_keeps_ud():
    url = "https://www.koreaherald.com/view.php?ud=20241018050378&np=1&mpv=0"
    norm = normalize_url(url)
    assert norm == "https://www.koreaherald.com/view.php?ud=20241018050378"


def test_normalize_theqoo_strips_page_param():
    url = "https://theqoo.net/square/4005485178?page=6"
    norm = normalize_url(url)
    assert norm == "https://theqoo.net/square/4005485178"


def test_normalize_generic_id_and_drops_other_params():
    url = "https://example.com/article?no=123&page=5&foo=bar"
    norm = normalize_url(url)
    assert norm == "https://example.com/article?no=123"
