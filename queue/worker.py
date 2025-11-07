# queue/worker.py

import time
import traceback

from .db import fetch_next_pending_job, update_job, add_to_dlq
from .job import Job, JOB_PENDING, JOB_PROCESSING, JOB_COMPLETED, JOB_FAILED
from .utils import logger, exponential_backoff
from .manager import QueueManager
from .config import WORKER_POLL_INTERVAL


class Worker:
    def __init__(self, poll_interval: int = None):
        self.poll_interval = poll_interval if poll_interval is not None else WORKER_POLL_INTERVAL
        self.manager = QueueManager()
        logger.info("[WORKER] initialized")

    # ---------------------------------------------------------
    # MAIN LOOP
    # ---------------------------------------------------------
    def start(self):
        logger.info("[WORKER] started")
        while True:
            row = fetch_next_pending_job()
            if not row:
                time.sleep(self.poll_interval)
                continue

            job = Job.from_dict(row)
            logger.info(f"[WORKER] picked job {job.id}: {job.command}")

            self._process(job)

    # ---------------------------------------------------------
    # PROCESS ONE JOB
    # ---------------------------------------------------------
    def _process(self, job: Job):
        # Mark job as processing in DB
        job.mark_processing()
        update_job(job)

        try:
            result = job.execute()
            logger.info(f"[WORKER] Job {job.id} SUCCESS -> {str(result)[:200]}")
            job.mark_completed()
            update_job(job)
            return

        except Exception as e:
            logger.error(
                f"[WORKER] Job {job.id} FAILED: {e}\n{traceback.format_exc()}"
            )

            # Increment attempt count
            job.attempts += 1

            # Retry if within allowed retries
            if job.attempts <= job.max_retries:
                job.mark_failed()  # updates timestamp & keeps in FAILED state
                update_job(job)

                delay = exponential_backoff(job.attempts)
                logger.warning(
                    f"[WORKER] RETRY {job.id} in {delay:.2f}s "
                    f"(attempt {job.attempts}/{job.max_retries})"
                )

                time.sleep(delay)
                return

            # Otherwise → DLQ
            logger.error(f"[WORKER] Job {job.id} moved to DLQ (max retries exceeded)")
            job.mark_dead()
            update_job(job)
            add_to_dlq(job)
