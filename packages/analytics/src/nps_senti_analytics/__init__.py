"""Analytics stage skeleton."""

from nps_senti_core import Config

from .correlate import run as run_correlate
from .events import run as run_events
from .trends import run as run_trends

__all__ = ["run", "run_correlate", "run_events", "run_trends"]


def run(cfg: Config) -> None:
    """Placeholder analytics pipeline."""

    raise NotImplementedError("Analytics pipeline not implemented yet.")
