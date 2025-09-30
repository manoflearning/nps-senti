from __future__ import annotations

from collections.abc import Iterable

from nps_senti_core import Config

from . import register


@register("dcinside-nps-gallery")
def fetch_dcinside_gallery(_: Config) -> Iterable[dict[str, object]]:
    """Placeholder for the DCInside crawler."""

    raise NotImplementedError("DCInside crawler not implemented yet.")
