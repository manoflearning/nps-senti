from __future__ import annotations

from collections.abc import Iterable

from nps_senti_core import Config

from . import register


@register("naver-news")
def fetch_naver_news(_: Config) -> Iterable[dict[str, object]]:
    """Placeholder for the Naver News crawler."""

    raise NotImplementedError("Naver crawler not implemented yet.")
