from __future__ import annotations
import platform
from pathlib import Path


VIZ_DIR = Path(__file__).resolve().parent.parent
ROOT_DIR = VIZ_DIR.parent

DATA_PATH = str(ROOT_DIR / "sentiment_output_data")
STOPWORDS_EN_PATH = str(VIZ_DIR / "stopwords-en.txt")


def _pick_font_path() -> str | None:
    sys = platform.system()

    # Windows / macOS
    if sys == "Windows":
        p = Path("C:/Windows/Fonts/malgun.ttf")
        return str(p) if p.exists() else None
    if sys == "Darwin":
        p = Path("/System/Library/Fonts/AppleSDGothicNeo.ttc")
        return str(p) if p.exists() else None

    # Linux: 흔한 한글 폰트 후보들
    candidates = [
        "/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/truetype/noto/NotoSansCJKkr-Regular.otf",
        "/usr/share/fonts/truetype/nanum/NanumGothic.ttf",
        "/usr/share/fonts/truetype/nanum/NanumGothicCoding.ttf",
        "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
        "/usr/share/fonts/opentype/noto/NotoSansCJKkr-Regular.otf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",  # 최후(한글은 □로 나올 수 있음)
    ]
    for c in candidates:
        if Path(c).exists():
            return c
    return None


FONT_PATH = _pick_font_path()

SENTIMENT_OPTIONS = ["negative", "neutral", "positive"]
ARTICLE_SOURCES = {"gdelt"}