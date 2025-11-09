# queue/worker.py

import time
import traceback

from .db import fetch_next_pending_job, update_job, add_to_dlq
from .job import Job, JOB_PENDING, JOB_PROCESSING, JOB_COMPLETED, JOB_FAILED, JOB_DEAD
from .utils import logger
from .manager import QueueManager
from .config import WORKER_POLL_INTERVAL


class Worker:
    def __init__(self, poll_interval: int = None):
        self.poll_interval = poll_interval if poll_interval is not None else WORKER_POLL_INTERVAL
        self.manager = QueueManager()
        logger.info("[WORKER] initialized")

    def start(self):
        logger.info("[WORKER] started")
        while True:
            row = fetch_next_pending_job()
            if not row:
                time.sleep(0.1)
                continue

            job = Job.from_dict(row)
            logger.info(f"[WORKER] picked job {job.id}: {job.command}")
            self._process(job)

    def _process(self, job: Job):
        # mark processing (in-memory) and persist
        job.mark_processing()
        update_job(job)

        try:
            # (optional compatibility) convert some shorthand module-style commands
            # into CLI python invocations — you kept this in prior iterations, keep if needed:
            # e.g. "jobs.add 2 3" -> "python jobs/add.py 2 3"
            if job.command.startswith("jobs.add "):
                parts = job.command.split()
                if len(parts) == 3:
                    a, b = parts[1], parts[2]
                    job.command = f"python jobs/add.py {a} {b}"
                else:
                    raise ValueError("jobs.add requires two numeric args")

            result = job.execute()
            logger.info(f"[WORKER] Job {job.id} SUCCESS -> {str(result)[:200]}")
            job.mark_completed()
            update_job(job)
            return

        except Exception as e:
            logger.error(f"[WORKER] Job {job.id} FAILED: {e}\n{traceback.format_exc()}")

            # single source of truth for attempts increment:
            job.mark_failed()   # increments attempts by 1 and sets failed state

            # If still allowed retries, set back to pending for re-queue
            if job.attempts <= job.max_retries:
                job.state = JOB_PENDING
                update_job(job)

                delay = 0.1  # very short for tests; replace by exponential_backoff(job.attempts) in prod
                logger.warning(f"[WORKER] RETRY {job.id} in {delay:.2f}s (attempt {job.attempts}/{job.max_retries})")
                time.sleep(delay)
                return

            # Exceeded retries -> DLQ
            logger.error(f"[WORKER] Job {job.id} moved to DLQ (max retries exceeded)")
            job.mark_dead()
            update_job(job)
            add_to_dlq(job)
            return
