from importlib import metadata


try:
    __version__ = metadata.version("national-pension-sentiment-analysis")
except metadata.PackageNotFoundError:
    __version__ = "0.1.0"
