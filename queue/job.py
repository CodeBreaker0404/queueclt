# queue/job.py
import uuid
import json
import subprocess
import importlib
from dataclasses import dataclass, asdict
from typing import Optional, Dict, Any

from .utils import now_timestamp, logger
from .config import MAX_RETRIES

# Job states
JOB_PENDING = "pending"
JOB_PROCESSING = "processing"
JOB_COMPLETED = "completed"
JOB_FAILED = "failed"
JOB_DEAD = "dead"


@dataclass
class Job:
    id: str
    command: str
    payload: Optional[str] = None      # JSON string
    mode: str = "cli"                  # "cli" or "python" — default to CLI
    state: str = JOB_PENDING
    attempts: int = 0
    max_retries: int = MAX_RETRIES
    created_at: str = ""
    updated_at: str = ""

    def __post_init__(self):
        if not self.created_at:
            self.created_at = now_timestamp()
        if not self.updated_at:
            self.updated_at = now_timestamp()

    # Dynamic property — computed, not stored
    @property
    def is_dynamic(self) -> bool:
        return self.mode == "python"

    # ------------------------
    # State transitions
    # ------------------------
    def mark_processing(self):
        self.state = JOB_PROCESSING
        self.updated_at = now_timestamp()

    def mark_completed(self):
        self.state = JOB_COMPLETED
        self.updated_at = now_timestamp()

    def mark_failed(self):
        self.state = JOB_FAILED
        self.attempts += 1
        self.updated_at = now_timestamp()

    def mark_dead(self):
        self.state = JOB_DEAD
        self.updated_at = now_timestamp()

    # ------------------------
    # Execution dispatcher
    # ------------------------
    def execute(self) -> Any:
        logger.info(f"[Job {self.id}] Executing (mode={self.mode}, dynamic={self.is_dynamic})")
        if self.is_dynamic:
            return self._execute_dynamic()
        return self._execute_cli()

    # ------------------------
    # CLI jobs
    # ------------------------
    def _execute_cli(self):
        logger.info(f"[Job {self.id}] CLI: {self.command}")
        result = subprocess.run(
            self.command,
            shell=True,
            capture_output=True,
            text=True,
            timeout=300  # prevent hanging
        )
        if result.returncode != 0:
            error_msg = result.stderr.strip() or "No stderr"
            logger.error(f"[Job {self.id}] CLI error: {error_msg}")
            raise subprocess.CalledProcessError(
                result.returncode, self.command,
                output=result.stdout, stderr=result.stderr
            )
        stdout = result.stdout.strip()
        logger.info(f"[Job {self.id}] CLI success: {stdout}")
        return stdout

    # ------------------------
    # Python jobs (dynamic)
    # ------------------------
    def _execute_dynamic(self):
        if "." not in self.command:
            raise ValueError(f"Invalid Python command format: {self.command}")

        module_path, func_name = self.command.rsplit(".", 1)

        try:
            module = importlib.import_module(module_path)
        except ModuleNotFoundError as e:
            raise ValueError(f"Module not found: {module_path}") from e

        func = getattr(module, func_name, None)
        if func is None or not callable(func):
            raise ValueError(f"Function '{func_name}' not found or not callable in module '{module_path}'")

        params = {}
        if self.payload:
            try:
                params = json.loads(self.payload)
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON in payload: {self.payload}") from e

        logger.info(f"[Job {self.id}] Running {self.command}(**{params})")
        try:
            result = func(**params)
            logger.info(f"[Job {self.id}] Python function returned: {result}")
            return result
        except Exception as e:
            logger.error(f"[Job {self.id}] Python function raised: {e}")
            raise

    # ------------------------
    # Serialization
    # ------------------------
    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d.pop("is_dynamic", None)
        return d

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "Job":
        return Job(
            id=d["id"],
            command=d["command"],
            payload=d.get("payload"),
            mode=d.get("mode", "cli"),
            state=d.get("state", JOB_PENDING),
            attempts=int(d.get("attempts", 0)),
            max_retries=int(d.get("max_retries", MAX_RETRIES)),
            created_at=d.get("created_at", now_timestamp()),
            updated_at=d.get("updated_at", now_timestamp()),
        )


# ------------------------
# Factory
# ------------------------
def create_job(
    command: str,
    payload: Optional[dict] = None,
    max_retries: Optional[int] = None,
    mode: str = "cli"
) -> Job:
    payload_json = json.dumps(payload) if payload else None

    return Job(
        id=str(uuid.uuid4()),
        command=command,
        payload=payload_json,
        mode=mode,
        max_retries=max_retries if max_retries is not None else MAX_RETRIES,
    )
