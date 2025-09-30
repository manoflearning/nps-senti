from __future__ import annotations

from collections.abc import Iterable

from nps_senti_core import Config

from . import register


@register("seed")
def fetch_seed(_: Config) -> Iterable[dict[str, object]]:
    """Placeholder seed crawler."""

    raise NotImplementedError("Seed crawler not implemented yet.")
