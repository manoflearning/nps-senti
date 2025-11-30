from __future__ import annotations

from dataclasses import dataclass
import os

# ✅ .env 자동 로딩
try:
    from dotenv import load_dotenv

    load_dotenv()  # 프로젝트 루트(.env)를 읽어서 os.environ에 주입
except Exception:
    # python-dotenv가 없어도 그냥 넘어가게 (이미 설치했으니까 보통은 여기 안 옴)
    pass

# 기본값들 (환경변수로 덮어쓸 수 있게 설계)
DEFAULT_MODEL = os.getenv("XAI_MODEL", "grok-4-fast-reasoning")
DEFAULT_API_BASE = os.getenv("XAI_API_BASE", "https://api.x.ai/v1")
ENV_API_KEYS = ("XAI_API_KEY", "GROK_API_KEY")


@dataclass(frozen=True)
class GrokConfig:
    api_key: str
    model: str = DEFAULT_MODEL
    api_base: str = DEFAULT_API_BASE
    timeout: float = float(os.getenv("XAI_TIMEOUT", "1200"))  # 초 단위


def load_config(model: str | None = None) -> GrokConfig:
    """
    환경변수에서 xAI(Grok) API 설정을 읽어온다.
    우선 XAI_API_KEY, 없으면 GROK_API_KEY 순서로 찾는다.
    """
    api_key = None
    for name in ENV_API_KEYS:
        value = os.getenv(name)
        if value:
            api_key = value
            break

    if not api_key:
        # 디버깅용으로 어떤 키들이 보이는지 한 번 출력 (stderr)
        visible_keys = [k for k in os.environ.keys() if "XAI" in k or "GROK" in k]
        raise RuntimeError(
            "xAI API key not found. 환경변수 XAI_API_KEY 또는 GROK_API_KEY 를 설정해 주세요.\n"
            f"현재 보이는 관련 키들: {visible_keys}"
        )

    return GrokConfig(
        api_key=api_key,
        model=model or DEFAULT_MODEL,
    )