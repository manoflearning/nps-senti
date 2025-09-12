from __future__ import annotations

import logging


def get_logger(name: str = "nps") -> logging.Logger:
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
        )
    return logging.getLogger(name)
