"""Machine learning stage skeleton."""

from nps_senti_core import Config

from .export_onnx import run as run_export_onnx
from .featurize import run as run_featurize
from .infer import run as run_infer
from .train import run as run_train

__all__ = ["run_export_onnx", "run_featurize", "run_infer", "run_train"]


def run(cfg: Config) -> None:  # type: ignore[unused-ignore]
    """Placeholder aggregate ML pipeline."""

    raise NotImplementedError("ML pipeline not implemented yet.")
