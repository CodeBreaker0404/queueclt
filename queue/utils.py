# queue/utils.py
import os
import math
import logging
from datetime import datetime
from .config import LOG_DIR, LOG_LEVEL, RETRY_BACKOFF_BASE

# Ensure logs dir
os.makedirs(LOG_DIR, exist_ok=True)

def get_logger(name: str):
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    level = getattr(logging, LOG_LEVEL.upper(), logging.INFO)
    logger.setLevel(level)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                            datefmt="%Y-%m-%d %H:%M:%S")
    fh = logging.FileHandler(os.path.join(LOG_DIR, f"{name}.log"))
    fh.setFormatter(fmt)
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger

# global logger instance
logger = get_logger("queue")

def exponential_backoff(attempt: int) -> float:
    """
    base ** attempt, with attempt starting at 1 for first retry.
    """
    # ensure attempt >=1 for meaningful backoff in caller
    a = max(1, attempt)
    return math.pow(RETRY_BACKOFF_BASE, a)

def now_timestamp() -> str:
    return datetime.utcnow().isoformat() + "Z"

def truncate_output(output: str, limit: int = 300) -> str:
    if output is None:
        return ""
    s = str(output)
    return s if len(s) <= limit else s[:limit] + "...(truncated)"

def is_valid_command(cmd: str) -> bool:
    return isinstance(cmd, str) and len(cmd.strip()) > 0
 