from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Callable, List, Optional
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.models.job import JobStatus, JobType, WorkflowJob
from src.repositories.job_repository import JobRepository

logger = logging.getLogger(__name__)


@dataclass
class JobTransitionEvent:
    interaction_id: str
    job_id: str
    customer_id: str
    job_type: str
    old_status: str
    new_status: str
    error: Optional[str]
    timestamp: str


class JobService:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        repository: Optional[JobRepository] = None,
        retry_base_seconds: int = 10,
        retry_max_seconds: int = 300,
    ):
        self._session_factory = session_factory
        self._repository = repository or JobRepository()
        self._retry_base_seconds = retry_base_seconds
        self._retry_max_seconds = retry_max_seconds

    async def enqueue_job(
        self,
        *,
        interaction_id: UUID,
        customer_id: UUID,
        job_type: JobType,
        payload: dict,
        priority: int = 100,
        max_attempts: int = 5,
        next_run_at: Optional[datetime] = None,
    ) -> WorkflowJob:
        when = next_run_at or datetime.now(timezone.utc)
        async with self._session_factory() as session:
            async with session.begin():
                job = await self._repository.enqueue_job(
                    session,
                    interaction_id=interaction_id,
                    customer_id=customer_id,
                    job_type=job_type,
                    payload=payload,
                    priority=priority,
                    max_attempts=max_attempts,
                    next_run_at=when,
                )
                self._log_transition(
                    job=job,
                    old_status="NEW",
                    new_status=JobStatus.PENDING.value,
                    error=None,
                )
                return job

    async def claim_next_jobs(
        self,
        *,
        worker_id: str,
        limit: int = 10,
        now: Optional[datetime] = None,
    ) -> List[WorkflowJob]:
        claim_at = now or datetime.now(timezone.utc)
        async with self._session_factory() as session:
            async with session.begin():
                jobs = await self._repository.claim_next_jobs(
                    session,
                    worker_id=worker_id,
                    limit=limit,
                    now=claim_at,
                )
                for job in jobs:
                    self._log_transition(
                        job=job,
                        old_status=JobStatus.PENDING.value
                        if job.attempts == 1
                        else JobStatus.RETRY.value,
                        new_status=JobStatus.RUNNING.value,
                        error=None,
                    )
                return jobs

    async def mark_job_completed(self, *, job_id: UUID) -> None:
        await self._transition(
            job_id=job_id,
            new_status=JobStatus.COMPLETED,
            error=None,
        )

    async def mark_job_failed(self, *, job_id: UUID, error: str) -> None:
        async with self._session_factory() as session:
            async with session.begin():
                job = await self._repository.get_job(session, job_id)
                if job is None:
                    return
                old_status = job.status.value
                if job.attempts >= job.max_attempts:
                    await self._repository.update_job_status(
                        session,
                        job_id=job.id,
                        status=JobStatus.DEAD_LETTER,
                        last_error=error,
                    )
                    self._log_transition(
                        job=job,
                        old_status=old_status,
                        new_status=JobStatus.DEAD_LETTER.value,
                        error=error,
                    )
                    return

                next_run_at = self._compute_next_run_at(job.attempts)
                await self._repository.update_job_status(
                    session,
                    job_id=job.id,
                    status=JobStatus.RETRY,
                    last_error=error,
                    next_run_at=next_run_at,
                )
                self._log_transition(
                    job=job,
                    old_status=old_status,
                    new_status=JobStatus.RETRY.value,
                    error=error,
                )

    async def reschedule_job(self, *, job_id: UUID, next_run_at: datetime, error: str) -> None:
        await self._transition(
            job_id=job_id,
            new_status=JobStatus.RETRY,
            error=error,
            next_run_at=next_run_at,
        )

    async def move_to_dead_letter(self, *, job_id: UUID, error: str) -> None:
        await self._transition(
            job_id=job_id,
            new_status=JobStatus.DEAD_LETTER,
            error=error,
        )

    async def recover_abandoned_running_jobs(
        self, *, lock_timeout_seconds: int = 300
    ) -> List[UUID]:
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(seconds=lock_timeout_seconds)
        async with self._session_factory() as session:
            async with session.begin():
                recovered = await self._repository.recover_stale_running_jobs(
                    session,
                    lock_timeout_before=cutoff,
                    new_next_run_at=now,
                )
                for job_id in recovered:
                    logger.warning(
                        "job_recovered_from_stale_lock",
                        extra={
                            "job_id": str(job_id),
                            "old_status": JobStatus.RUNNING.value,
                            "new_status": JobStatus.RETRY.value,
                            "timestamp": now.isoformat(),
                        },
                    )
                return recovered

    async def _transition(
        self,
        *,
        job_id: UUID,
        new_status: JobStatus,
        error: Optional[str],
        next_run_at: Optional[datetime] = None,
    ) -> None:
        async with self._session_factory() as session:
            async with session.begin():
                job = await self._repository.get_job(session, job_id)
                if job is None:
                    return
                old_status = job.status.value
                await self._repository.update_job_status(
                    session,
                    job_id=job.id,
                    status=new_status,
                    last_error=error,
                    next_run_at=next_run_at,
                )
                self._log_transition(
                    job=job,
                    old_status=old_status,
                    new_status=new_status.value,
                    error=error,
                )

    def _compute_next_run_at(self, attempts: int) -> datetime:
        backoff = min(
            self._retry_max_seconds,
            self._retry_base_seconds * (2 ** max(0, attempts - 1)),
        )
        return datetime.now(timezone.utc) + timedelta(seconds=backoff)

    def _log_transition(
        self, *, job: WorkflowJob, old_status: str, new_status: str, error: Optional[str]
    ) -> None:
        event = JobTransitionEvent(
            interaction_id=str(job.interaction_id),
            job_id=str(job.id),
            customer_id=str(job.customer_id),
            job_type=job.job_type.value if hasattr(job.job_type, "value") else str(job.job_type),
            old_status=old_status,
            new_status=new_status,
            error=error,
            timestamp=datetime.now(timezone.utc).isoformat(),
        )
        logger.info("job_state_transition", extra=event.__dict__)
