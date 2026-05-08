import os
import asyncio
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from src.models.base import Base
from src.models.job import JobStatus, JobType, WorkflowJob
from src.repositories.job_repository import JobRepository
from src.config import settings


def _db_url() -> str:
    # Allow overriding in CI, otherwise use app default.
    return os.getenv("DATABASE_URL", settings.DATABASE_URL)


@pytest_asyncio.fixture(scope="function")
async def pg_engine():
    engine = create_async_engine(_db_url(), pool_pre_ping=True)
    try:
        yield engine
    finally:
        await engine.dispose()


@pytest.fixture(scope="function")
def pg_session_factory(pg_engine):
    return async_sessionmaker(pg_engine, expire_on_commit=False)


@pytest.fixture(autouse=True)
async def ensure_workflow_jobs_table(pg_engine):
    try:
        async with pg_engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
    except OSError as exc:
        pytest.skip(
            f"Postgres not reachable for integration tests ({exc}). "
            "Run `docker compose up -d` and ensure DATABASE_URL points to it."
        )
    yield


@pytest.fixture(autouse=True)
async def truncate_workflow_jobs(pg_session_factory):
    try:
        async with pg_session_factory() as session:
            async with session.begin():
                await session.execute(text("TRUNCATE TABLE workflow_jobs"))
    except OSError as exc:
        pytest.skip(
            f"Postgres not reachable for integration tests ({exc}). "
            "Run `docker compose up -d` and ensure DATABASE_URL points to it."
        )
    yield


@pytest.mark.asyncio
async def test_claim_next_jobs_concurrent_skip_locked(pg_session_factory):
    repo = JobRepository()
    now = datetime.now(timezone.utc)

    # Insert 10 eligible jobs.
    async with pg_session_factory() as session:
        async with session.begin():
            for i in range(10):
                job = WorkflowJob(
                    interaction_id=uuid4(),
                    customer_id=uuid4(),
                    job_type=JobType.RECORDING_ORCHESTRATION,
                    status=JobStatus.PENDING,
                    payload={"n": i},
                    priority=100,
                    attempts=0,
                    max_attempts=5,
                    next_run_at=now,
                )
                session.add(job)

    async def claim(worker: str, limit: int):
        async with pg_session_factory() as session:
            async with session.begin():
                return await repo.claim_next_jobs(
                    session,
                    worker_id=worker,
                    limit=limit,
                    now=now,
                )

    c1, c2 = await asyncio.gather(claim("w1", 7), claim("w2", 7))
    ids1 = {j.id for j in c1}
    ids2 = {j.id for j in c2}

    assert ids1.isdisjoint(ids2)
    assert len(ids1 | ids2) == 10

    # Assert DB has exactly 10 RUNNING.
    async with pg_session_factory() as session:
        rows = await session.execute(
            text("SELECT count(*) FROM workflow_jobs WHERE status = 'RUNNING'")
        )
        assert rows.scalar_one() == 10


@pytest.mark.asyncio
async def test_recovery_sweeper_moves_stale_running_to_retry(pg_session_factory):
    repo = JobRepository()
    now = datetime.now(timezone.utc)
    stale = now - timedelta(minutes=10)

    async with pg_session_factory() as session:
        async with session.begin():
            job = WorkflowJob(
                interaction_id=uuid4(),
                customer_id=uuid4(),
                job_type=JobType.POSTCALL_ANALYSIS,
                status=JobStatus.RUNNING,
                payload={},
                priority=100,
                attempts=1,
                max_attempts=5,
                next_run_at=now,
                locked_by="dead-worker",
                locked_at=stale,
                last_error=None,
            )
            session.add(job)

    async with pg_session_factory() as session:
        async with session.begin():
            recovered = await repo.recover_stale_running_jobs(
                session,
                lock_timeout_before=now - timedelta(seconds=300),
                new_next_run_at=now,
            )

    assert len(recovered) == 1

    async with pg_session_factory() as session:
        row = await session.execute(
            text(
                "SELECT status, locked_by, locked_at FROM workflow_jobs LIMIT 1"
            )
        )
        status, locked_by, locked_at = row.first()
        assert status == "RETRY"
        assert locked_by is None
        assert locked_at is None

