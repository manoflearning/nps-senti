"""Visualization stage skeleton."""

from nps_senti_core import Config

from .charts import run as run_charts
from .dashboard import run as run_dashboard

__all__ = ["run", "run_charts", "run_dashboard"]


def run(cfg: Config) -> None:
    """Placeholder visualization pipeline."""

    raise NotImplementedError("Visualization pipeline not implemented yet.")
