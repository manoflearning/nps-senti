from __future__ import annotations

from pathlib import Path
import re

from wordcloud import STOPWORDS as WC_STOPWORDS

from .config import STOPWORDS_EN_PATH


def load_en_stopwords(path: str) -> set[str]:
    p = Path(path)
    if not p.exists():
        return set()
    words: set[str] = set()
    for line in p.read_text(encoding="utf-8").splitlines():
        w = line.strip().lower()
        if not w or w.startswith("#"):
            continue
        words.add(w)
    return words


_EN_STOPWORDS: set[str] | None = set(WC_STOPWORDS) | load_en_stopwords(
    STOPWORDS_EN_PATH
)


def get_en_stopwords() -> set[str]:
    global _EN_STOPWORDS
    if _EN_STOPWORDS is None:
        _EN_STOPWORDS = set(WC_STOPWORDS) | load_en_stopwords(STOPWORDS_EN_PATH)
    return _EN_STOPWORDS


def is_english_word(token: str) -> bool:
    return re.fullmatch(r"[A-Za-z]{2,}", token) is not None


def clean_text(text: str) -> str:
    text = re.sub(r"[^가-힣A-Za-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def is_korean_word(token: str) -> bool:
    return any("가" <= ch <= "힣" for ch in token)
