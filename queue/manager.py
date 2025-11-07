# queue/manager.py
from typing import List, Optional
from .db import init_db, insert_job, fetch_jobs, fetch_job_by_id, update_job
from .job import Job, create_job
from .utils import logger

class QueueManager:
    def __init__(self):
        init_db()

    def enqueue(
        self,
        command: str,
        payload: Optional[dict] = None,
        dynamic: bool = False,
        max_retries: Optional[int] = None,
        use_python: bool = False
    ) -> str:
        """
        Enqueue a job into SQLite queue.
        mode = "python" when --python flag is passed
        mode = "cli"    for default jobs
        """

        mode = "python" if use_python else "cli"

# queue/manager.py (inside enqueue method)

        job = create_job(
            command=command,
            payload=payload,
            max_retries=max_retries,
            mode="python" if use_python else "cli"   # <-- set mode instead of dynamic
        )


        insert_job(job)
        logger.info(f"[ENQUEUE] {job.id} (mode={job.mode}) -> {command}")
        print("DEBUG mode =", mode)

        return job.id

    def list_jobs(self) -> List[Job]:
        rows = fetch_jobs()
        return [Job.from_dict(r) for r in rows]

    def get_job(self, job_id: str) -> Optional[Job]:
        r = fetch_job_by_id(job_id)
        return Job.from_dict(r) if r else None

    # status updaters used by worker
    def mark_processing(self, job: Job):
        job.mark_processing()
        update_job(job)
        logger.debug(f"[STATUS] {job.id} -> processing")

    def mark_completed(self, job: Job):
        job.mark_completed()
        update_job(job)
        logger.debug(f"[STATUS] {job.id} -> completed")

    def mark_failed(self, job: Job):
        job.mark_failed()
        update_job(job)
        logger.debug(f"[STATUS] {job.id} -> failed (attempt {job.attempts})")

    def mark_dead(self, job: Job):
        job.mark_dead()
        update_job(job)
        logger.debug(f"[STATUS] {job.id} -> dead")
