from typing import List, Any
from datetime import datetime, timezone
import uuid
from dataclasses import dataclass, field

from src.db.writer import upsert_run_meta, upsert_run_step_meta

@dataclass
class RunContext:
    run_day: str
    commit_hash: str
    conn: Any
    started_at: datetime = field(init=False)

    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))

    def __enter__(self):
        self.started_at = datetime.now(timezone.utc)
        # Create the run record (status: in_progress)
        upsert_run_meta(
            conn=self.conn,
            run_id=self.run_id,
            run_day=self.run_day,
            commit_hash=self.commit_hash,
            started_at=self.started_at.isoformat(),
            ended_at=None,
            duration_ms=None,
            status="in_progress",
            error_message=None,
            error_type=None
        )
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        ended_at = datetime.now(timezone.utc)
        duration_ms = int((ended_at - self.started_at).total_seconds() * 1000)
        status = "completed" if exc_type is None else "failed"
        error_message = str(exc_value)[:1000] if exc_value else None
        error_type = exc_type.__name__ if exc_type else None

        # Update the run record
        upsert_run_meta(
            conn=self.conn,
            run_id=self.run_id,
            run_day=self.run_day,
            commit_hash=self.commit_hash,
            started_at=self.started_at.isoformat(),
            ended_at=ended_at.isoformat(),
            duration_ms=duration_ms,
            status=status,
            error_message=error_message,
            error_type=error_type,
        )

        return False # Propagate exceptions

    def get_run_id(self) -> str:
        return self.run_id

@dataclass
class StepContext:
    run_id : str
    step_name : str
    inputs : List[str]
    outputs : List[str]
    conn : Any

    started_at : datetime = field(init=False)
    success_count : int = 0
    error_count : int = 0
    step_run_id : str = field(default_factory=lambda: str(uuid.uuid4()))

    def __enter__(self):
        self.started_at = datetime.now(timezone.utc)
        # Create the run_step record (status: in_progress)
        upsert_run_step_meta(
            conn=self.conn,
            run_id=self.run_id,
            step_run_id=self.step_run_id,
            step_name=self.step_name,
            success_count=None,
            error_count=None,
            started_at=self.started_at.isoformat(),
            ended_at=None,
            duration_ms=None,
            status="in_progress",
            inputs=self.inputs,
            outputs=self.outputs,
            error_message=None,
            error_type=None
        )
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        ended_at = datetime.now(timezone.utc)
        duration_ms = int((ended_at - self.started_at).total_seconds() * 1000)
        status = "completed" if exc_type is None else "failed"
        error_message = str(exc_value) if exc_value else None
        error_type = exc_type.__name__ if exc_type else None

        if self.error_count == 0 and self.success_count == 0:
            error_count = None
            success_count = None
        else:
            error_count = self.error_count
            success_count = self.success_count

        # Update the run_step record
        upsert_run_step_meta(
            conn=self.conn,
            run_id=self.run_id,
            step_run_id=self.step_run_id,
            step_name=self.step_name,
            success_count=success_count,
            error_count=error_count,
            started_at=self.started_at.isoformat(),
            ended_at=ended_at.isoformat(),
            duration_ms=duration_ms,
            status=status,
            inputs=self.inputs,
            outputs=self.outputs,
            error_message=error_message,
            error_type=error_type,
        )
        return False # Propagate exceptions