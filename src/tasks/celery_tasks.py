import asyncio
import logging
import time
from datetime import datetime
from typing import Any, Dict

from src.config import settings
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

logger = logging.getLogger(__name__)


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


async def _retrieve_recording(payload: Dict[str, Any]) -> None:
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
        return

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
            return

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
        orchestrate_postcall_pipeline_task.apply_async(args=[payload], countdown=delay)
        return

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
        analysis_payload = dict(payload)
        analysis_payload["recording_s3_key"] = s3_key
        run_postcall_analysis_task.apply_async(
            args=[analysis_payload],
            queue=settings.POSTCALL_CELERY_QUEUE,
        )
        return

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
        return

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
    orchestrate_postcall_pipeline_task.apply_async(args=[payload], countdown=delay)


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


# Backward-compatible aliases.
retrieve_recording_background_task = orchestrate_postcall_pipeline_task
process_interaction_analysis_background_task = run_postcall_analysis_task
process_interaction_end_background_task = orchestrate_postcall_pipeline_task
