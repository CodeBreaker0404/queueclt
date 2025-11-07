# queue/config.py
import os
import json

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_FILE = os.path.join(BASE_DIR, "config.json")

# Defaults (module-level)
DB_PATH = os.path.join(BASE_DIR, "queue.db")
LOG_DIR = os.path.join(BASE_DIR, "logs")
LOG_LEVEL = "INFO"
RETRY_BACKOFF_BASE = 2.0
MAX_RETRIES = 3
METRICS_ENABLED = False
METRICS_INTERVAL = 10
WORKER_POLL_INTERVAL = 2

DEFAULT_CONFIG = {
    "db_path": DB_PATH,
    "log_dir": LOG_DIR,
    "log_level": LOG_LEVEL,
    "retry_backoff_base": RETRY_BACKOFF_BASE,
    "max_retries": MAX_RETRIES,
    "metrics_enabled": METRICS_ENABLED,
    "metrics_interval": METRICS_INTERVAL,
    "worker_poll_interval": WORKER_POLL_INTERVAL,
}

def load_config():
    """
    Load config.json if present and update module-level variables.
    Returns the final config dict.
    """
    global DB_PATH, LOG_DIR, LOG_LEVEL, RETRY_BACKOFF_BASE
    global MAX_RETRIES, METRICS_ENABLED, METRICS_INTERVAL, WORKER_POLL_INTERVAL

    cfg = DEFAULT_CONFIG.copy()
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r") as f:
                file_cfg = json.load(f)
            cfg.update(file_cfg)
        except Exception:
            # ignore parsing errors and keep defaults
            pass

    DB_PATH = cfg.get("db_path", DB_PATH)
    LOG_DIR = cfg.get("log_dir", LOG_DIR)
    LOG_LEVEL = cfg.get("log_level", LOG_LEVEL)
    RETRY_BACKOFF_BASE = float(cfg.get("retry_backoff_base", RETRY_BACKOFF_BASE))
    MAX_RETRIES = int(cfg.get("max_retries", MAX_RETRIES))
    METRICS_ENABLED = bool(cfg.get("metrics_enabled", METRICS_ENABLED))
    METRICS_INTERVAL = int(cfg.get("metrics_interval", METRICS_INTERVAL))
    WORKER_POLL_INTERVAL = int(cfg.get("worker_poll_interval", WORKER_POLL_INTERVAL))

    return cfg
