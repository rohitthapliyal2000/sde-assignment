from dataclasses import asdict
from unittest.mock import AsyncMock

import pytest

from src.services.recording import RecordingStatus
from src.services.recording_state import RecordingState
from src.tasks import celery_tasks


class InMemoryRecordingStateStore:
    def __init__(self):
        self._states = {}

    async def init_pending(self, interaction_id: str, payload: dict):
        state = RecordingState(
            interaction_id=interaction_id,
            status=RecordingStatus.PENDING.value,
            attempts=0,
            started_at=1.0,
            updated_at=1.0,
            payload=payload,
        )
        self._states[interaction_id] = state
        return state

    async def get(self, interaction_id: str):
        return self._states.get(interaction_id)

    async def update(
        self,
        interaction_id: str,
        status: RecordingStatus,
        attempts=None,
        next_retry_at=None,
        last_error=None,
        recording_s3_key=None,
    ):
        state = self._states[interaction_id]
        state.status = status.value
        if attempts is not None:
            state.attempts = attempts
        state.next_retry_at = next_retry_at
        state.last_error = last_error
        if recording_s3_key:
            state.recording_s3_key = recording_s3_key
        self._states[interaction_id] = state
        return state


def _payload(interaction_id: str = "i-1") -> dict:
    return {
        "interaction_id": interaction_id,
        "session_id": "s-1",
        "lead_id": "l-1",
        "campaign_id": "c-1",
        "customer_id": "cust-1",
        "agent_id": "a-1",
        "call_sid": "call-1",
        "transcript_text": "agent: hi\ncustomer: hello",
        "conversation_data": {"transcript": []},
        "additional_data": {},
        "ended_at": "2026-01-01T00:00:00",
        "exotel_account_id": "exotel-1",
    }


@pytest.mark.asyncio
async def test_recording_delayed_availability_enqueues_analysis_after_available(monkeypatch):
    store = InMemoryRecordingStateStore()
    monkeypatch.setattr(celery_tasks, "recording_state_store", store)
    monkeypatch.setattr(celery_tasks.settings, "RECORDING_MAX_ATTEMPTS", 4)
    monkeypatch.setattr(celery_tasks.settings, "RECORDING_BACKOFF_BASE_SECONDS", 2)
    monkeypatch.setattr(celery_tasks.settings, "RECORDING_BACKOFF_MAX_SECONDS", 20)
    monkeypatch.setattr(
        celery_tasks,
        "fetch_and_upload_recording_once",
        AsyncMock(side_effect=[None, "recordings/i-1.mp3"]),
    )
    monkeypatch.setattr(celery_tasks.metrics_tracker, "track_recording_retry", AsyncMock())
    monkeypatch.setattr(
        celery_tasks.metrics_tracker, "track_recording_terminal_state", AsyncMock()
    )

    payload = _payload("i-1")
    first = await celery_tasks._retrieve_recording(payload)
    state_after_first = await store.get("i-1")
    assert state_after_first.status == RecordingStatus.RETRYING.value
    assert state_after_first.attempts == 1
    assert first["state"] == RecordingStatus.RETRYING.value
    assert first["retry_delay_seconds"] > 0

    second = await celery_tasks._retrieve_recording(payload)
    state_after_second = await store.get("i-1")
    assert state_after_second.status == RecordingStatus.AVAILABLE.value
    assert state_after_second.attempts == 2
    assert state_after_second.recording_s3_key == "recordings/i-1.mp3"
    assert second["state"] == RecordingStatus.AVAILABLE.value


@pytest.mark.asyncio
async def test_recording_permanent_timeout_reaches_terminal_state(monkeypatch):
    store = InMemoryRecordingStateStore()
    monkeypatch.setattr(celery_tasks, "recording_state_store", store)
    monkeypatch.setattr(celery_tasks.settings, "RECORDING_MAX_ATTEMPTS", 2)
    monkeypatch.setattr(celery_tasks.settings, "RECORDING_BACKOFF_BASE_SECONDS", 1)
    monkeypatch.setattr(celery_tasks.settings, "RECORDING_BACKOFF_MAX_SECONDS", 10)
    monkeypatch.setattr(
        celery_tasks, "fetch_and_upload_recording_once", AsyncMock(return_value=None)
    )
    monkeypatch.setattr(celery_tasks.metrics_tracker, "track_recording_retry", AsyncMock())
    monkeypatch.setattr(
        celery_tasks.metrics_tracker, "track_recording_terminal_state", AsyncMock()
    )

    payload = _payload("i-timeout")
    first = await celery_tasks._retrieve_recording(payload)
    second = await celery_tasks._retrieve_recording(payload)
    final_state = await store.get("i-timeout")
    assert final_state.status == RecordingStatus.TIMEOUT.value
    assert final_state.attempts == 2
    assert first["state"] == RecordingStatus.RETRYING.value
    assert second["state"] == RecordingStatus.TIMEOUT.value


@pytest.mark.asyncio
async def test_worker_restart_recovery_uses_persisted_attempts(monkeypatch):
    store = InMemoryRecordingStateStore()
    persisted = RecordingState(
        interaction_id="i-restart",
        status=RecordingStatus.RETRYING.value,
        attempts=2,
        started_at=1.0,
        updated_at=2.0,
        next_retry_at=3.0,
        last_error="recording_not_ready",
        payload=_payload("i-restart"),
    )
    store._states["i-restart"] = RecordingState(**asdict(persisted))

    monkeypatch.setattr(celery_tasks, "recording_state_store", store)
    monkeypatch.setattr(celery_tasks.settings, "RECORDING_MAX_ATTEMPTS", 4)
    monkeypatch.setattr(celery_tasks.settings, "RECORDING_BACKOFF_BASE_SECONDS", 2)
    monkeypatch.setattr(celery_tasks.settings, "RECORDING_BACKOFF_MAX_SECONDS", 20)
    monkeypatch.setattr(
        celery_tasks, "fetch_and_upload_recording_once", AsyncMock(return_value=None)
    )
    monkeypatch.setattr(celery_tasks.metrics_tracker, "track_recording_retry", AsyncMock())
    monkeypatch.setattr(
        celery_tasks.metrics_tracker, "track_recording_terminal_state", AsyncMock()
    )

    result = await celery_tasks._retrieve_recording(_payload("i-restart"))
    recovered_state = await store.get("i-restart")
    assert recovered_state.attempts == 3
    assert recovered_state.status == RecordingStatus.RETRYING.value
    assert result["state"] == RecordingStatus.RETRYING.value
