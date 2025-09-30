from importlib import metadata


PACKAGE_NAME = "nps-senti"

try:
    __version__ = metadata.version(PACKAGE_NAME)
except metadata.PackageNotFoundError:
    __version__ = "0.1.0"

__all__ = ["__version__"]
