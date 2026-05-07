import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional
from uuid import UUID, uuid4

import pytest

from src.models.job import JobStatus, JobType
from src.services.job_service import JobService


class DummySession:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    def begin(self):
        return self


class DummySessionFactory:
    def __call__(self):
        return DummySession()


@dataclass
class InMemoryJob:
    id: UUID
    interaction_id: UUID
    customer_id: UUID
    job_type: JobType
    status: JobStatus
    payload: dict
    priority: int
    attempts: int
    max_attempts: int
    next_run_at: datetime
    locked_by: Optional[str] = None
    locked_at: Optional[datetime] = None
    last_error: Optional[str] = None
    created_at: datetime = datetime.now(timezone.utc)


class InMemoryJobRepository:
    def __init__(self):
        self.jobs: Dict[UUID, InMemoryJob] = {}
        self._lock = asyncio.Lock()

    async def enqueue_job(
        self,
        session,
        *,
        interaction_id,
        customer_id,
        job_type,
        payload,
        priority,
        max_attempts,
        next_run_at,
    ):
        job = InMemoryJob(
            id=uuid4(),
            interaction_id=interaction_id,
            customer_id=customer_id,
            job_type=job_type,
            status=JobStatus.PENDING,
            payload=payload,
            priority=priority,
            attempts=0,
            max_attempts=max_attempts,
            next_run_at=next_run_at,
            created_at=datetime.now(timezone.utc),
        )
        self.jobs[job.id] = job
        return job

    async def get_job(self, session, job_id):
        return self.jobs.get(job_id)

    async def claim_next_jobs(self, session, *, worker_id, limit, now):
        async with self._lock:
            eligible = [
                job
                for job in self.jobs.values()
                if job.status in {JobStatus.PENDING, JobStatus.RETRY}
                and job.next_run_at <= now
            ]
            eligible.sort(key=lambda j: (-j.priority, j.next_run_at, j.created_at))
            picked = eligible[:limit]
            for job in picked:
                job.status = JobStatus.RUNNING
                job.attempts += 1
                job.locked_by = worker_id
                job.locked_at = now
            return picked

    async def update_job_status(
        self,
        session,
        *,
        job_id,
        status,
        last_error=None,
        next_run_at=None,
        clear_lock=True,
    ):
        job = self.jobs[job_id]
        job.status = status
        job.last_error = last_error
        if next_run_at is not None:
            job.next_run_at = next_run_at
        if clear_lock:
            job.locked_by = None
            job.locked_at = None

    async def recover_stale_running_jobs(self, session, *, lock_timeout_before, new_next_run_at):
        recovered = []
        for job in self.jobs.values():
            if job.status == JobStatus.RUNNING and job.locked_at and job.locked_at < lock_timeout_before:
                job.status = JobStatus.RETRY
                job.locked_by = None
                job.locked_at = None
                job.next_run_at = new_next_run_at
                recovered.append(job.id)
        return recovered


def make_service(repo: InMemoryJobRepository) -> JobService:
    return JobService(
        DummySessionFactory(),
        repository=repo,
        retry_base_seconds=5,
        retry_max_seconds=60,
    )


@pytest.mark.asyncio
async def test_concurrent_claiming_no_duplicate_execution():
    repo = InMemoryJobRepository()
    svc = make_service(repo)
    now = datetime.now(timezone.utc)
    for _ in range(6):
        await svc.enqueue_job(
            interaction_id=uuid4(),
            customer_id=uuid4(),
            job_type=JobType.RECORDING_ORCHESTRATION,
            payload={"x": 1},
            next_run_at=now,
        )

    c1, c2 = await asyncio.gather(
        svc.claim_next_jobs(worker_id="w1", limit=4, now=now),
        svc.claim_next_jobs(worker_id="w2", limit=4, now=now),
    )
    ids1 = {j.id for j in c1}
    ids2 = {j.id for j in c2}
    assert ids1.isdisjoint(ids2)
    assert len(ids1 | ids2) == 6


@pytest.mark.asyncio
async def test_retry_scheduling_sets_retry_state_and_future_time():
    repo = InMemoryJobRepository()
    svc = make_service(repo)
    job = await svc.enqueue_job(
        interaction_id=uuid4(),
        customer_id=uuid4(),
        job_type=JobType.POSTCALL_ANALYSIS,
        payload={},
        next_run_at=datetime.now(timezone.utc),
    )
    await svc.claim_next_jobs(worker_id="w1", limit=1, now=datetime.now(timezone.utc))
    await svc.mark_job_failed(job_id=job.id, error="transient_error")
    updated = repo.jobs[job.id]
    assert updated.status == JobStatus.RETRY
    assert updated.next_run_at > datetime.now(timezone.utc)
    assert updated.last_error == "transient_error"


@pytest.mark.asyncio
async def test_dead_letter_after_max_attempts():
    repo = InMemoryJobRepository()
    svc = make_service(repo)
    job = await svc.enqueue_job(
        interaction_id=uuid4(),
        customer_id=uuid4(),
        job_type=JobType.POSTCALL_ANALYSIS,
        payload={},
        max_attempts=1,
        next_run_at=datetime.now(timezone.utc),
    )
    await svc.claim_next_jobs(worker_id="w1", limit=1, now=datetime.now(timezone.utc))
    await svc.mark_job_failed(job_id=job.id, error="permanent_error")
    assert repo.jobs[job.id].status == JobStatus.DEAD_LETTER


@pytest.mark.asyncio
async def test_worker_crash_recovery_moves_stale_running_to_retry():
    repo = InMemoryJobRepository()
    svc = make_service(repo)
    job = await svc.enqueue_job(
        interaction_id=uuid4(),
        customer_id=uuid4(),
        job_type=JobType.RECORDING_ORCHESTRATION,
        payload={},
        next_run_at=datetime.now(timezone.utc),
    )
    repo.jobs[job.id].status = JobStatus.RUNNING
    repo.jobs[job.id].locked_by = "stale-worker"
    repo.jobs[job.id].locked_at = datetime.now(timezone.utc) - timedelta(minutes=10)

    recovered = await svc.recover_abandoned_running_jobs(lock_timeout_seconds=300)
    assert job.id in recovered
    assert repo.jobs[job.id].status == JobStatus.RETRY


@pytest.mark.asyncio
async def test_db_source_of_truth_resilience_without_queue_delivery():
    repo = InMemoryJobRepository()
    svc = make_service(repo)
    now = datetime.now(timezone.utc)
    job = await svc.enqueue_job(
        interaction_id=uuid4(),
        customer_id=uuid4(),
        job_type=JobType.RECORDING_ORCHESTRATION,
        payload={"queued_in_redis": False},
        next_run_at=now,
    )
    claimed = await svc.claim_next_jobs(worker_id="w1", limit=1, now=now)
    assert len(claimed) == 1
    assert claimed[0].id == job.id
