from __future__ import annotations

from functools import lru_cache
from pathlib import Path
import re

from wordcloud import STOPWORDS as WC_STOPWORDS

from .config import STOPWORDS_EN_PATH


# -----------------------------
# English stopwords loader
# -----------------------------
def load_en_stopwords(path: str) -> set[str]:
    p = Path(path)
    if not p.exists():
        return set()

    words: set[str] = set()
    for line in p.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s:
            continue
        if s.startswith("#"):
            continue
        for w in re.split(r"\s+", s):
            w = w.strip().lower()
            if w:
                words.add(w)
    return words


@lru_cache(maxsize=1)
def get_en_stopwords() -> set[str]:
    return set(map(str.lower, WC_STOPWORDS)) | load_en_stopwords(STOPWORDS_EN_PATH)


# -----------------------------
# Token helpers
# -----------------------------
def is_english_word(token: str) -> bool:
    return re.fullmatch(r"[A-Za-z]{2,}", token) is not None


def is_korean_word(token: str) -> bool:
    return any("가" <= ch <= "힣" for ch in token)


# -----------------------------
# Noise stripping for wordcloud
# -----------------------------
_RE_URL = re.compile(r"(https?://\S+|www\.\S+)", re.IGNORECASE)
_RE_EMAIL = re.compile(r"\b[\w.\-+]+@[\w.\-]+\.\w+\b", re.IGNORECASE)

# Thread/Threads, Instagram, Twitter 등 “홍보 링크” 라인 제거용
_RE_SOCIAL_PREFIX = re.compile(
    r"^\s*(thread|threads|instagram|facebook|twitter|tiktok|kakaotalk|email|e-mail)\s*[:\-]\s*",
    re.IGNORECASE,
)

# 저작권/재배포 같은 boilerplate 라인 제거용
_RE_BOILERPLATE = re.compile(
    r"(all\s+rights\s+reserved|copyright|unauthorized\s+reproduction|reproduction|redistribution|"
    r"무단\s*(전재|복제|재배포)|전재|재배포|ai\s*학습|학습\s*이용\s*금지)",
    re.IGNORECASE,
)

_RE_DC_APP = re.compile(r"\bdc\s+official\s+app\b", re.IGNORECASE)


def strip_web_noise(text: str) -> str:
    """
    워드클라우드용 텍스트에서:
    - URL / email 제거
    - Thread(s): ... 같은 고정 홍보 줄 제거
    - 저작권/재배포/AI학습금지 boilerplate 줄 제거
    - 'dc official app' 제거
    """
    if not text:
        return ""

    t = str(text)
    t = t.replace("\r\n", "\n").replace("\r", "\n")

    kept: list[str] = []
    for line in t.split("\n"):
        s = line.strip()
        if not s:
            continue

        # 소셜 홍보 라인 제거
        if _RE_SOCIAL_PREFIX.search(s) and (_RE_URL.search(s) or "@" in s or "threads.com" in s.lower()):
            continue

        # boilerplate 라인 제거
        if _RE_BOILERPLATE.search(s):
            continue

        kept.append(s)

    t = " ".join(kept)

    # 남은 텍스트에서 URL/email 제거
    t = _RE_URL.sub(" ", t)
    t = _RE_EMAIL.sub(" ", t)

    # dc official app 꼬리표 제거
    t = _RE_DC_APP.sub(" ", t)

    # 공백 정리
    t = re.sub(r"\s+", " ", t).strip()
    return t


def clean_text(text: str) -> str:
    """
    토큰화 전에 최소한의 정리:
    - 특수문자 제거 / 공백 정리
    """
    text = re.sub(r"[^가-힣A-Za-z0-9\s]", " ", str(text))
    text = re.sub(r"\s+", " ", text)
    return text.strip()


# -----------------------------
# Heuristics: gibberish English token filter
# -----------------------------
_VOWELS = set("aeiou")

_RE_HAS_DIGIT = re.compile(r"\d")
_RE_CONSONANT_RUN = re.compile(r"[^aeiou]{5,}", re.IGNORECASE)


def _vowel_count(tok: str) -> int:
    return sum(1 for ch in tok if ch in _VOWELS)


def is_gibberish_en(token: str, *, whitelist: set[str] | None = None) -> bool:
    """
    워드클라우드에 섞이는 '난수/ID/조각' 영어 토큰 제거용 휴리스틱.
    (목표: fxgiyxj, ucgorq, hhpletitxdqzg, bwbkg, ewsdq 류 제거)

    - 숫자 혼합(letters+digits) 또는 너무 긴 토큰 제거
    - 모음이 지나치게 적은 토큰 제거
    - 자음연속 5자 이상(길고 난수인 경우) 제거
    - 1~2글자 약어는 이미 is_english_word에서 2+만 남기니 여기서는 과도 제거만 피함
    """
    if not token:
        return True

    t = token.lower().strip()
    if whitelist and t in whitelist:
        return False

    # 알파벳 외가 섞이면(정상 단어 아님)
    if not re.fullmatch(r"[a-z]+", t):
        return True

    # 너무 긴 것(채널 id/해시/도메인조각 가능성 높음)
    if len(t) >= 22:
        return True

    # digits가 섞인 형태는 여기까지 못 오지만, 안전망
    if _RE_HAS_DIGIT.search(t):
        return True

    v = _vowel_count(t)
    L = len(t)

    # 모음 거의 없음 + 길다 => 난수 확률 매우 높음
    # (ex: bwbkg, ucgorq, fxgiyxj)
    if L >= 7 and v <= 1:
        return True

    # 모음 비율이 너무 낮고 길면 난수로 판단
    # (ex: hhpletitxdqzg -> v=2, L=12, ratio=0.166)
    if L >= 10 and (v / L) < 0.22:
        return True

    # 긴 자음 연속 (난수/도메인 조각에 흔함)
    if L >= 9 and _RE_CONSONANT_RUN.search(t):
        return True

    # 같은 글자 과도 반복(스팸 조각)
    if L >= 8 and max(t.count(ch) for ch in set(t)) >= 5:
        return True

    return False
