import json
import time
from dataclasses import asdict, dataclass
from typing import Any, Dict, Optional

from src.services.recording import RecordingStatus
from src.utils.redis_client import redis_client

RECORDING_STATE_PREFIX = "postcall:recording_state:"


@dataclass
class RecordingState:
    interaction_id: str
    status: str
    attempts: int
    started_at: float
    updated_at: float
    next_retry_at: Optional[float] = None
    last_error: Optional[str] = None
    recording_s3_key: Optional[str] = None
    payload: Optional[Dict[str, Any]] = None


class RecordingStateStore:
    def __init__(self, ttl_seconds: int = 86400):
        self.ttl_seconds = ttl_seconds

    async def init_pending(self, interaction_id: str, payload: Dict[str, Any]) -> RecordingState:
        now = time.time()
        state = RecordingState(
            interaction_id=interaction_id,
            status=RecordingStatus.PENDING.value,
            attempts=0,
            started_at=now,
            updated_at=now,
            payload=payload,
        )
        await self._save(state)
        return state

    async def get(self, interaction_id: str) -> Optional[RecordingState]:
        raw = await redis_client.get(self._key(interaction_id))
        if not raw:
            return None
        return RecordingState(**json.loads(raw))

    async def update(
        self,
        interaction_id: str,
        status: RecordingStatus,
        attempts: Optional[int] = None,
        next_retry_at: Optional[float] = None,
        last_error: Optional[str] = None,
        recording_s3_key: Optional[str] = None,
    ) -> RecordingState:
        current = await self.get(interaction_id)
        now = time.time()
        if current is None:
            current = RecordingState(
                interaction_id=interaction_id,
                status=status.value,
                attempts=attempts or 0,
                started_at=now,
                updated_at=now,
            )

        current.status = status.value
        current.updated_at = now
        if attempts is not None:
            current.attempts = attempts
        current.next_retry_at = next_retry_at
        current.last_error = last_error
        if recording_s3_key:
            current.recording_s3_key = recording_s3_key

        await self._save(current)
        return current

    def _key(self, interaction_id: str) -> str:
        return f"{RECORDING_STATE_PREFIX}{interaction_id}"

    async def _save(self, state: RecordingState) -> None:
        await redis_client.set(
            self._key(state.interaction_id),
            json.dumps(asdict(state)),
            ex=self.ttl_seconds,
        )


recording_state_store = RecordingStateStore()
