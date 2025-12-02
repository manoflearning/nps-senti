from __future__ import annotations
import platform
from pathlib import Path


VIZ_DIR = Path(__file__).resolve().parent.parent
ROOT_DIR = VIZ_DIR.parent

DATA_PATH = str(ROOT_DIR / "sentiment_output_data")
STOPWORDS_EN_PATH = str(VIZ_DIR / "stopwords-en.txt")


if platform.system() == "Windows":
    FONT_PATH = "C:/Windows/Fonts/malgun.ttf"
else:
    FONT_PATH = "/System/Library/Fonts/AppleSDGothicNeo.ttc"


SENTIMENT_OPTIONS = ["negative", "neutral", "positive"]
ARTICLE_SOURCES = {"gdelt"}
