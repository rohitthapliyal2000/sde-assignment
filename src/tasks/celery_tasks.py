import asyncio
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict
from uuid import UUID, NAMESPACE_DNS, uuid5

from src.config import settings
from src.models.job import JobStatus, JobType
from src.services.job_service import JobService
from src.services.metrics import metrics_tracker
from src.tasks.celery_app import celery_app
from src.services.post_call_processor import PostCallProcessor, PostCallContext
from src.services.recording import (
    RecordingStatus,
    compute_backoff_seconds,
    fetch_and_upload_recording_once,
)
from src.services.recording_state import recording_state_store
from src.services.signal_jobs import trigger_signal_jobs, update_lead_stage
from src.services.retry_queue import retry_queue
from src.utils.db import async_session_factory
from sqlalchemy import text
from src.utils.redis_client import redis_client

logger = logging.getLogger(__name__)
job_service = JobService(
    async_session_factory,
    retry_base_seconds=settings.WORKFLOW_RETRY_BASE_SECONDS,
    retry_max_seconds=settings.WORKFLOW_RETRY_MAX_SECONDS,
)


@celery_app.task(
    name="orchestrate_postcall_pipeline_task",
    bind=True,
    max_retries=0,
    acks_late=True,
    queue="postcall_processing",
)
def orchestrate_postcall_pipeline_task(self, payload: Dict[str, Any]):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(_retrieve_recording(payload))
    except Exception as e:
        logger.exception("recording_task_crashed", extra={"error": str(e)})
        raise
    finally:
        loop.close()


@celery_app.task(
    name="run_postcall_analysis_task",
    bind=True,
    max_retries=3,
    default_retry_delay=60,
    acks_late=True,
    queue="postcall_processing",
)
def run_postcall_analysis_task(self, payload: Dict[str, Any]):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(_process_interaction_analysis(self, payload))
    except Exception as e:
        logger.exception(
            "analysis_task_failed",
            extra={
                "interaction_id": payload.get("interaction_id"),
                "error": str(e),
                "attempt": self.request.retries,
            },
        )
        loop.run_until_complete(
            retry_queue.enqueue_retry(
                interaction_id=payload["interaction_id"],
                error=str(e),
                payload=payload,
            )
        )
        raise self.retry(exc=e)
    finally:
        loop.close()


async def _retrieve_recording(payload: Dict[str, Any]) -> Dict[str, Any]:
    interaction_id = payload["interaction_id"]
    state = await recording_state_store.get(interaction_id)
    if state is None:
        state = await recording_state_store.init_pending(interaction_id, payload)
        logger.info(
            "recording_state_initialized",
            extra={"interaction_id": interaction_id, "status": state.status},
        )

    if state.status in {
        RecordingStatus.AVAILABLE.value,
        RecordingStatus.TIMEOUT.value,
        RecordingStatus.FAILED.value,
    }:
        logger.info(
            "recording_task_skipped_terminal_state",
            extra={"interaction_id": interaction_id, "status": state.status},
        )
        return {"state": state.status}

    attempt = state.attempts + 1
    logger.info(
        "recording_attempt_started",
        extra={"interaction_id": interaction_id, "attempt": attempt, "status": state.status},
    )
    try:
        s3_key = await fetch_and_upload_recording_once(
            interaction_id=interaction_id,
            call_sid=payload.get("call_sid", ""),
            exotel_account_id=payload.get("exotel_account_id") or "",
        )
    except Exception as exc:
        if attempt >= settings.RECORDING_MAX_ATTEMPTS:
            await recording_state_store.update(
                interaction_id=interaction_id,
                status=RecordingStatus.FAILED,
                attempts=attempt,
                last_error=str(exc),
            )
            await metrics_tracker.track_recording_terminal_state(
                interaction_id, RecordingStatus.FAILED.value, attempt, str(exc)
            )
            logger.error(
                "recording_failed_terminal",
                extra={"interaction_id": interaction_id, "attempt": attempt, "error": str(exc)},
            )
            return {"state": RecordingStatus.FAILED.value, "error": str(exc)}

        delay = compute_backoff_seconds(
            attempt=attempt,
            base_seconds=settings.RECORDING_BACKOFF_BASE_SECONDS,
            max_seconds=settings.RECORDING_BACKOFF_MAX_SECONDS,
        )
        next_retry_at = time.time() + delay
        await recording_state_store.update(
            interaction_id=interaction_id,
            status=RecordingStatus.RETRYING,
            attempts=attempt,
            next_retry_at=next_retry_at,
            last_error=str(exc),
        )
        await metrics_tracker.track_recording_retry(interaction_id, attempt, delay)
        logger.warning(
            "recording_retry_scheduled",
            extra={
                "interaction_id": interaction_id,
                "attempt": attempt,
                "next_retry_in_seconds": delay,
                "error": str(exc),
            },
        )
        return {
            "state": RecordingStatus.RETRYING.value,
            "retry_delay_seconds": delay,
            "error": str(exc),
        }

    if s3_key:
        await recording_state_store.update(
            interaction_id=interaction_id,
            status=RecordingStatus.AVAILABLE,
            attempts=attempt,
            recording_s3_key=s3_key,
            last_error=None,
            next_retry_at=None,
        )
        await metrics_tracker.track_recording_terminal_state(
            interaction_id, RecordingStatus.AVAILABLE.value, attempt
        )
        logger.info(
            "recording_available_enqueuing_analysis",
            extra={"interaction_id": interaction_id, "attempt": attempt, "s3_key": s3_key},
        )
        return {"state": RecordingStatus.AVAILABLE.value, "recording_s3_key": s3_key}

    if attempt >= settings.RECORDING_MAX_ATTEMPTS:
        await recording_state_store.update(
            interaction_id=interaction_id,
            status=RecordingStatus.TIMEOUT,
            attempts=attempt,
            next_retry_at=None,
            last_error="recording_not_ready_within_retry_budget",
        )
        await metrics_tracker.track_recording_terminal_state(
            interaction_id,
            RecordingStatus.TIMEOUT.value,
            attempt,
            "recording_not_ready_within_retry_budget",
        )
        logger.error(
            "recording_timeout_terminal",
            extra={"interaction_id": interaction_id, "attempt": attempt},
        )
        return {
            "state": RecordingStatus.TIMEOUT.value,
            "error": "recording_not_ready_within_retry_budget",
        }

    delay = compute_backoff_seconds(
        attempt=attempt,
        base_seconds=settings.RECORDING_BACKOFF_BASE_SECONDS,
        max_seconds=settings.RECORDING_BACKOFF_MAX_SECONDS,
    )
    next_retry_at = time.time() + delay
    await recording_state_store.update(
        interaction_id=interaction_id,
        status=RecordingStatus.RETRYING,
        attempts=attempt,
        next_retry_at=next_retry_at,
        last_error="recording_not_ready",
    )
    await metrics_tracker.track_recording_retry(interaction_id, attempt, delay)
    logger.info(
        "recording_not_ready_retry_scheduled",
        extra={
            "interaction_id": interaction_id,
            "attempt": attempt,
            "next_retry_in_seconds": delay,
        },
    )
    return {
        "state": RecordingStatus.RETRYING.value,
        "retry_delay_seconds": delay,
        "error": "recording_not_ready",
    }


async def _process_interaction_analysis(task, payload: Dict[str, Any]):
    interaction_id = payload["interaction_id"]

    await metrics_tracker.track_processing_started(interaction_id)

    ctx = PostCallContext(
        interaction_id=interaction_id,
        session_id=payload["session_id"],
        lead_id=payload["lead_id"],
        campaign_id=payload["campaign_id"],
        customer_id=payload["customer_id"],
        agent_id=payload["agent_id"],
        call_sid=payload.get("call_sid", ""),
        transcript_text=payload.get("transcript_text", ""),
        conversation_data=payload.get("conversation_data", {}),
        additional_data=payload.get("additional_data", {}),
        ended_at=datetime.fromisoformat(payload["ended_at"]),
        exotel_account_id=payload.get("exotel_account_id"),
    )

    processor = PostCallProcessor()
    result = await processor.process_post_call(ctx, single_prompt=True)

    await metrics_tracker.track_processing_completed(
        interaction_id, result.tokens_used, result.latency_ms
    )

    # ── Step 3: Signal jobs ───────────────────────────────────────────────────
    # Downstream actions: send a WhatsApp follow-up, book a callback slot,
    # push to the customer's CRM. These depend on knowing the analysis result.
    #
    # If this raises, we log a warning and continue — the lead stage still
    # updates. But the downstream action (WhatsApp, callback, CRM push) is lost.
    try:
        await trigger_signal_jobs(
            interaction_id=ctx.interaction_id,
            session_id=ctx.session_id,
            campaign_id=ctx.campaign_id,
            analysis_result=result.raw_response,
        )
    except Exception as e:
        logger.warning("signal_jobs_failed", extra={"error": str(e)})

    # ── Step 4: Lead stage update ─────────────────────────────────────────────
    # Updates the lead's stage in the leads table based on call_stage.
    # e.g., "rebook_confirmed" → lead moves to "booked" stage.
    # Same fire-and-forget risk as signal_jobs above.
    try:
        await update_lead_stage(
            lead_id=ctx.lead_id,
            interaction_id=ctx.interaction_id,
            call_stage=result.call_stage,
        )
    except Exception as e:
        logger.warning("lead_stage_update_failed", extra={"error": str(e)})


async def enqueue_postcall_workflow_job(payload: Dict[str, Any]) -> str:
    interaction_uuid = _coerce_uuid(payload["interaction_id"])
    customer_uuid = _coerce_uuid(payload["customer_id"])
    job = await job_service.enqueue_job(
        interaction_id=interaction_uuid,
        customer_id=customer_uuid,
        job_type=JobType.RECORDING_ORCHESTRATION,
        payload=payload,
        priority=payload.get("priority", 100),
        max_attempts=settings.RECORDING_MAX_ATTEMPTS,
    )
    return str(job.id)


def _coerce_uuid(value: str) -> UUID:
    try:
        return UUID(str(value))
    except ValueError:
        return uuid5(NAMESPACE_DNS, str(value))


@celery_app.task(name="drain_due_workflow_jobs_task", bind=True, queue="postcall_processing")
def drain_due_workflow_jobs_task(self):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(_drain_due_jobs())
    finally:
        loop.close()


@celery_app.task(name="recover_stale_workflow_jobs_task", bind=True, queue="postcall_processing")
def recover_stale_workflow_jobs_task(self):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(
            job_service.recover_abandoned_running_jobs(
                lock_timeout_seconds=settings.WORKFLOW_LOCK_TIMEOUT_SECONDS
            )
        )
    finally:
        loop.close()


async def _drain_due_jobs() -> None:
    worker_id = f"celery:{time.time_ns()}"
    claimed = await job_service.claim_next_jobs(
        worker_id=worker_id,
        limit=settings.WORKFLOW_CLAIM_BATCH_SIZE,
    )
    # Best-effort queue visibility for throttling (DB is source of truth).
    try:
        async with async_session_factory() as session:
            pending = await session.execute(
                text(
                    "SELECT count(*) FROM workflow_jobs WHERE status IN ('PENDING','RETRY')"
                )
            )
            retry = await session.execute(
                text("SELECT count(*) FROM workflow_jobs WHERE status = 'RETRY'")
            )
            await redis_client.set("workflow:queue_depth", int(pending.scalar_one()), ex=30)
            await redis_client.set("workflow:retry_depth", int(retry.scalar_one()), ex=30)
    except Exception:
        # Visibility should never break processing.
        pass
    for job in claimed:
        await _execute_claimed_job(job)


async def _execute_claimed_job(job) -> None:
    payload = dict(job.payload or {})
    try:
        if job.job_type == JobType.RECORDING_ORCHESTRATION:
            outcome = await _retrieve_recording(payload)
            state = outcome.get("state")
            if state == RecordingStatus.AVAILABLE.value:
                analysis_payload = dict(payload)
                analysis_payload["recording_s3_key"] = outcome.get("recording_s3_key")
                await job_service.enqueue_job(
                    interaction_id=job.interaction_id,
                    customer_id=job.customer_id,
                    job_type=JobType.POSTCALL_ANALYSIS,
                    payload=analysis_payload,
                    priority=max(job.priority - 10, 1),
                    max_attempts=settings.POSTCALL_MAX_RETRIES,
                )
                await job_service.mark_job_completed(job_id=job.id)
                return

            if state == RecordingStatus.RETRYING.value:
                delay = int(outcome.get("retry_delay_seconds", 0))
                await job_service.reschedule_job(
                    job_id=job.id,
                    next_run_at=datetime.now(timezone.utc) + timedelta(seconds=delay),
                    error=outcome.get("error", "retry_scheduled"),
                )
                return

            if state in {RecordingStatus.TIMEOUT.value, RecordingStatus.FAILED.value}:
                await job_service.move_to_dead_letter(
                    job_id=job.id,
                    error=outcome.get("error", state.lower()),
                )
                return

            await job_service.mark_job_completed(job_id=job.id)
            return

        if job.job_type == JobType.POSTCALL_ANALYSIS:
            await _process_interaction_analysis(None, payload)
            await job_service.mark_job_completed(job_id=job.id)
            return

        await job_service.mark_job_failed(
            job_id=job.id,
            error=f"unknown_job_type:{job.job_type}",
        )
    except Exception as exc:
        await job_service.mark_job_failed(job_id=job.id, error=str(exc))


# Backward-compatible aliases.
retrieve_recording_background_task = orchestrate_postcall_pipeline_task
process_interaction_analysis_background_task = run_postcall_analysis_task
process_interaction_end_background_task = orchestrate_postcall_pipeline_task
