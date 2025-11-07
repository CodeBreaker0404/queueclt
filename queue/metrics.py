# queue/metrics.py
import time
import threading
from typing import Dict
from .utils import logger
from .config import METRICS_ENABLED, METRICS_INTERVAL

class Metrics:
    def __init__(self):
        self.total_processed = 0
        self.total_failed = 0
        self.total_exec_time = 0.0
        self.worker_heartbeats = {}
        self.lock = threading.Lock()
        if METRICS_ENABLED:
            t = threading.Thread(target=self._poll, daemon=True)
            t.start()

    def job_success(self, exec_time: float):
        with self.lock:
            self.total_processed += 1
            self.total_exec_time += exec_time

    def job_failure(self):
        with self.lock:
            self.total_failed += 1

    def active_workers(self) -> int:
        now = time.time()
        with self.lock:
            return sum(1 for t in self.worker_heartbeats.values() if now - t <= 10)

    def heartbeat(self, wid: int):
        with self.lock:
            self.worker_heartbeats[wid] = time.time()

    def export(self):
        with self.lock:
            avg = (self.total_exec_time / self.total_processed) if self.total_processed else 0.0
            return {
                "processed": self.total_processed,
                "failed": self.total_failed,
                "avg_exec_time": round(avg, 4),
                "active_workers": self.active_workers()
            }

    def _poll(self):
        while True:
            time.sleep(METRICS_INTERVAL)
            logger.info(f"[METRICS] {self.export()}")

metrics = Metrics()
