# queue/dlq.py
from typing import List, Dict
from .db import list_dlq, restore_dlq, delete_dlq
from .utils import logger

class DLQ:
    def list_all(self) -> List[Dict]:
        jobs = list_dlq()
        logger.info(f"[DLQ] {len(jobs)} entries")
        return jobs

    def retry(self, job_id: str) -> bool:
        ok = restore_dlq(job_id)
        if ok:
            logger.info(f"[DLQ] restored {job_id}")
        else:
            logger.warning(f"[DLQ] restore failed {job_id}")
        return ok

    def purge(self) -> int:
        jobs = list_dlq()
        cnt = 0
        for j in jobs:
            delete_dlq(j["id"])
            cnt += 1
        logger.warning(f"[DLQ] purged {cnt}")
        return cnt
