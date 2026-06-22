from __future__ import annotations

import logging


BAN_LEVEL = 25
logging.addLevelName(BAN_LEVEL, "BAN")


def ban(logger: logging.Logger, message: str, *args: object) -> None:
    logger.log(BAN_LEVEL, message, *args)


def configure_logging(level_name: str, *, ban_only: bool = False) -> logging.Logger:
    logger = logging.getLogger("qbt-hdd-guard")
    logger.handlers.clear()
    logger.setLevel(logging.DEBUG)

    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s (%(name)s) %(levelname)s %(message)s", "%Y.%m.%dT%H:%M:%S"))

    if ban_only:
        handler.setLevel(BAN_LEVEL)
    else:
        handler.setLevel(_level(level_name))

    logger.addHandler(handler)
    logger.propagate = False
    return logger


def _level(level_name: str) -> int:
    if level_name.upper() == "BAN":
        return BAN_LEVEL
    return int(getattr(logging, level_name.upper(), logging.INFO))
