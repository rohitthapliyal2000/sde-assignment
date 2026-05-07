from __future__ import annotations

from datetime import datetime, timezone
from typing import List, Optional
from uuid import UUID

from sqlalchemy import text
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.job import JobStatus, JobType, WorkflowJob


CLAIM_NEXT_JOBS_SQL = """
WITH candidate AS (
    SELECT id
    FROM workflow_jobs
    WHERE status IN ('PENDING', 'RETRY')
      AND next_run_at <= :now
    ORDER BY priority DESC, next_run_at ASC, created_at ASC
    FOR UPDATE SKIP LOCKED
    LIMIT :limit
)
UPDATE workflow_jobs w
SET status = 'RUNNING',
    attempts = w.attempts + 1,
    locked_by = :worker_id,
    locked_at = :now,
    updated_at = :now
FROM candidate
WHERE w.id = candidate.id
RETURNING w.id
"""


class JobRepository:
    async def enqueue_job(
        self,
        session: AsyncSession,
        *,
        interaction_id: UUID,
        customer_id: UUID,
        job_type: JobType,
        payload: dict,
        priority: int,
        max_attempts: int,
        next_run_at: datetime,
    ) -> WorkflowJob:
        job = WorkflowJob(
            interaction_id=interaction_id,
            customer_id=customer_id,
            job_type=job_type,
            status=JobStatus.PENDING,
            payload=payload,
            priority=priority,
            attempts=0,
            max_attempts=max_attempts,
            next_run_at=next_run_at,
        )
        session.add(job)
        await session.flush()
        await session.refresh(job)
        return job

    async def get_job(self, session: AsyncSession, job_id: UUID) -> Optional[WorkflowJob]:
        return await session.get(WorkflowJob, job_id)

    async def claim_next_jobs(
        self,
        session: AsyncSession,
        *,
        worker_id: str,
        limit: int,
        now: datetime,
    ) -> List[WorkflowJob]:
        rows = await session.execute(
            text(CLAIM_NEXT_JOBS_SQL),
            {"worker_id": worker_id, "limit": limit, "now": now},
        )
        claimed_ids = [row[0] for row in rows.fetchall()]
        if not claimed_ids:
            return []
        result = await session.execute(
            select(WorkflowJob)
            .where(WorkflowJob.id.in_(claimed_ids))
            .order_by(
                WorkflowJob.priority.desc(),
                WorkflowJob.next_run_at.asc(),
                WorkflowJob.created_at.asc(),
            )
        )
        return list(result.scalars().all())

    async def update_job_status(
        self,
        session: AsyncSession,
        *,
        job_id: UUID,
        status: JobStatus,
        last_error: Optional[str] = None,
        next_run_at: Optional[datetime] = None,
        clear_lock: bool = True,
    ) -> None:
        await session.execute(
            text(
                """
                UPDATE workflow_jobs
                SET status = :status,
                    last_error = :last_error,
                    next_run_at = COALESCE(:next_run_at, next_run_at),
                    updated_at = :updated_at,
                    locked_by = CASE WHEN :clear_lock THEN NULL ELSE locked_by END,
                    locked_at = CASE WHEN :clear_lock THEN NULL ELSE locked_at END
                WHERE id = :job_id
                """
            ),
            {
                "job_id": job_id,
                "status": status.value,
                "last_error": last_error,
                "next_run_at": next_run_at,
                "updated_at": datetime.now(timezone.utc),
                "clear_lock": clear_lock,
            },
        )

    async def recover_stale_running_jobs(
        self,
        session: AsyncSession,
        *,
        lock_timeout_before: datetime,
        new_next_run_at: datetime,
    ) -> List[UUID]:
        rows = await session.execute(
            text(
                """
                UPDATE workflow_jobs
                SET status = 'RETRY',
                    locked_by = NULL,
                    locked_at = NULL,
                    next_run_at = :new_next_run_at,
                    updated_at = :updated_at,
                    last_error = COALESCE(last_error, 'worker_abandoned_lock')
                WHERE status = 'RUNNING'
                  AND locked_at IS NOT NULL
                  AND locked_at < :lock_timeout_before
                RETURNING id
                """
            ),
            {
                "new_next_run_at": new_next_run_at,
                "lock_timeout_before": lock_timeout_before,
                "updated_at": datetime.now(timezone.utc),
            },
        )
        return [row[0] for row in rows.fetchall()]
