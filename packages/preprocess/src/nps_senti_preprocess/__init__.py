"""Preprocessing stage skeleton."""

from nps_senti_core import Config

from .clean import run as run_clean
from .dedup import run as run_dedup

__all__ = ["run", "run_clean", "run_dedup"]


def run(cfg: Config) -> None:
    """Placeholder preprocessing pipeline."""

    raise NotImplementedError("Preprocess pipeline not implemented yet.")
