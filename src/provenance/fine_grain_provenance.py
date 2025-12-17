from datetime import datetime, timezone
import uuid
from typing import Any
from dataclasses import dataclass, field

from src.db.writer import upsert_api_request


@dataclass
class RequestContext:
    run_id: str
    step_run_id: str
    source: str
    local_artist_id: str
    platform_id: str  # spotify_artist_id OR wiki_title OR youtube_channel_id
    conn: Any
    
    endpoint: str = field(default=None)
    http_status: int = field(default=None)
    params: dict[str, any] = field(default=None)
    request_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    url: str = ""
    started_at: datetime = field(init=False)

    def __enter__(self):
        self.started_at = datetime.now(timezone.utc)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        finished_at = datetime.now(timezone.utc)
        duration_ms = int((finished_at - self.started_at).total_seconds() * 1000)
        ok = 1 if exc_type is None else 0
        if exc_type:
            print(f"Exception in RequestContext: {exc_value}")
            error_type = exc_type.__name__ 
        else:
            error_type = None

        if exc_value:
            print(f"Exception value: {exc_value}")
            error_message = str(exc_value)
        else:
            error_message = None

        if self.http_status is None:
            print("Warning: http_status not set in RequestContext before exit.")
        if self.endpoint is None:
            print("Warning: endpoint not set in RequestContext before exit.")
        # Params can be None so no warning for that

        # Insert the api_request record
        upsert_api_request(
            conn=self.conn,
            run_id=self.run_id,
            step_run_id=self.step_run_id,
            request_id=self.request_id,
            source=self.source,
            local_artist_id=self.local_artist_id,
            platform_id=self.platform_id,
            endpoint=self.endpoint,
            request_params=self.params,
            requested_at=self.started_at.isoformat(),
            finished_at=finished_at.isoformat(),
            duration_ms=duration_ms,
            http_status=None,
            ok=ok,
            error_type=error_type,
            error_message=error_message
        )
        return False  # Propagate exceptions
    
    def set_http_status(self, http_status: int):
        self.http_status = http_status

    def set_params(self, params: dict[str, any]):
        self.params = dict(params)  # Make a copy to avoid mutation issues
    
    def set_endpoint(self, endpoint: str):
        self.endpoint = endpoint

