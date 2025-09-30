"""Topic modeling stage skeleton."""

from nps_senti_core import Config

from .keywords import run as run_keywords
from .models import run as run_topics

__all__ = ["run", "run_keywords", "run_topics"]


def run(cfg: Config) -> None:
    """Placeholder topics pipeline."""

    raise NotImplementedError("Topics pipeline not implemented yet.")
