from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from importlib import import_module
from pkgutil import iter_modules

from nps_senti_core import Config, get_logger

LOGGER = get_logger(__name__)

CrawlRecord = dict[str, object]
Fetcher = Callable[[Config], Iterable[CrawlRecord]]


@dataclass(frozen=True)
class Source:
    name: str
    fetch: Fetcher


_REGISTRY: dict[str, Source] = {}
_DISCOVERED = False


def register(name: str) -> Callable[[Fetcher], Fetcher]:
    def decorator(func: Fetcher) -> Fetcher:
        if name in _REGISTRY:
            raise ValueError(f"Crawler source '{name}' already registered")
        _REGISTRY[name] = Source(name=name, fetch=func)
        return func

    return decorator


def _discover_sources() -> None:
    global _DISCOVERED
    if _DISCOVERED:
        return
    package_name = __name__
    package_path = __path__  # type: ignore[name-defined]
    for module_info in iter_modules(package_path, prefix=f"{package_name}."):
        import_module(module_info.name)
    _DISCOVERED = True


def iter_sources() -> Iterable[Source]:
    _discover_sources()
    return _REGISTRY.values()


def fetch_all(cfg: Config) -> Iterable[CrawlRecord]:
    for source in iter_sources():
        try:
            yield from source.fetch(cfg)
        except Exception as exc:  # noqa: BLE001 - log and continue
            LOGGER.exception("Crawler '%s' failed: %s", source.name, exc)
            continue


__all__ = ["Source", "register", "iter_sources", "fetch_all"]
