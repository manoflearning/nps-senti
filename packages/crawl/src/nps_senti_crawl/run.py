from __future__ import annotations

from nps_senti_core import Config


def run(cfg: Config, *, overwrite: bool = False) -> None:
    """Placeholder crawl entrypoint.

    Implement this function to coordinate registered sources and persist
    aggregated crawl results.
    """

    raise NotImplementedError("Crawl pipeline not implemented yet.")


__all__ = ["run"]
