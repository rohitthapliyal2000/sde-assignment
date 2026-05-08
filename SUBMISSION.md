## Post-Call Processing Pipeline — Design Document

**Author:** Rohit Thapliyal  
**Date:** 2026-05-08  

---

## 1. Assumptions

1. **Call-end webhook SLA**: Telephony provider expects a response within ~5s; the webhook must enqueue work and return quickly.
2. **At-least-once is acceptable**: The system should prefer at-least-once processing over at-most-once; idempotency is required for side effects.
3. **LLM limits are hard**: LLM provider enforces both RPM and TPM; exceeding either yields 429s. We must schedule rather than “retry storm”.
4. **Multi-tenant fairness required**: Multiple customers can run campaigns concurrently; one customer’s surge must not starve others.
5. **Recording availability is delayed/variable**: Recording URL may appear 10–120s after call end; polling is allowed and safe.
6. **Postgres is available**: Postgres is a dependable source of truth. Redis is fast but not durable.
7. **Durability beats throughput**: Under overload, it’s acceptable to increase latency for low-priority analysis rather than dropping work.
8. **Priority is an input**: Campaign/business can provide a priority signal (or defaults). Higher priority gets processed earlier under pressure.
9. **LLM analysis is independent of recording availability**: Transcript-based LLM analysis does not depend on recording availability and can proceed independently.

---

## 2. Problem Diagnosis

The original system failed at scale due to:

- **Non-durable orchestration**: Celery broker backed by Redis loses tasks on restart; the “retry queue” is also Redis → double-loss.
- **Blocking recording sleep**: `asyncio.sleep(45)` stalls worker time regardless of recording readiness.
- **Binary circuit breaker**: A single threshold triggers a 30-minute stop, causing cascading failure (post-call backlog stops new calls).
- **No rate limit awareness**: LLM requests fire at full speed; 429s trigger retries that worsen backlog and latency.

---

## 3. Architecture Overview

### High level

- **FastAPI** remains the webhook receiver.
- **Postgres** becomes the authoritative workflow state store (`workflow_jobs`).
- **Celery** remains as an execution accelerator / periodic runner (beat) but **not** the source of truth.
- **Redis** is used for:
  - short-lived counters (RPM/TPM windows, throttling signals)
  - atomic limiter reservations (Lua)

### End-to-end diagram

```text
Exotel -> FastAPI webhook
  -> enqueue durable WorkflowJob(RECORDING_ORCHESTRATION) in Postgres (PENDING)
  -> trigger drain_due_workflow_jobs_task (Celery)

Celery drain task (and beat):
  -> claim_next_jobs() with SELECT ... FOR UPDATE SKIP LOCKED
  -> execute job handler
     - recording orchestration: polling/backoff, reschedule via next_run_at, terminal states
     - postcall analysis: reserve LLM capacity (global + per-customer RPM/TPM), defer using next_run_at
  -> write structured audit logs for transitions/decisions

Recovery:
  -> recover_stale_workflow_jobs_task periodically moves abandoned RUNNING -> RETRY
```

### Key design decisions

1. **Durable workflow jobs**: Postgres table tracks state; Redis outages don’t lose workflow state.
2. **Scheduled retries**: use `next_run_at` instead of “sleep in worker”.
3. **Rate limiting by reservation**: reserve capacity before LLM calls; deny means reschedule, not retry-storm.
4. **Graded backpressure**: throttle proportionally; reserve breaker for short outage windows only.

---

## 4. Rate Limit Management

### How rate limits are tracked

We track both **RPM** and **TPM** as sliding windows (Redis TTL-based keys):

- `llm:limit:global:rpm`, `llm:limit:global:tpm`
- `llm:limit:customer:{customer_id}:rpm`, `llm:limit:customer:{customer_id}:tpm`

All are updated via a **single atomic Lua script** to avoid race conditions across workers.

### How we decide process-now vs defer

Decision is made per `POSTCALL_ANALYSIS` job:

1. Estimate tokens for the request (initially `LLM_AVG_TOKENS_PER_CALL`).
2. Call `RedisRateLimiter.reserve(customer_id, rpm=1, tpm=tokens)`.
3. If allowed: execute LLM call.
4. If denied: compute `retry_after_seconds` (based on the max TTL of limiting keys) and **reschedule** the job:
   - `next_run_at = now + retry_after_seconds`

This ensures we do not create a backlog of immediate retries; we queue work until capacity refills.

### What happens when limits are hit

- **Capacity unavailable (reservation denied)**: reschedule using refill-based `next_run_at`.
- **Provider 429**: respect `Retry-After` (in real implementation) and requeue using that duration.
- **Other transient provider errors**: exponential backoff via workflow retry logic; repeated errors can trigger outage mode.

---

## 5. Per-Customer Token Budgeting

### Allocation model

We enforce per-customer limits with optional caps:

- Global caps:
  - `LLM_TOKENS_PER_MINUTE`, `LLM_REQUESTS_PER_MINUTE`
- Per-customer caps:
  - `LLM_PER_CUSTOMER_TOKENS_PER_MINUTE`
  - `LLM_PER_CUSTOMER_REQUESTS_PER_MINUTE`

If total TPM is \(N\) and active customers are \(K\), budgets can be set as:

- **Guaranteed minimum** per customer: pre-allocated TPM/RPM (configured).
- **Borrowing headroom**: if per-customer caps are 0 (disabled), customer can use spare global capacity.

### Guarantees and overages

- If Customer A is allocated 20 TPM and uses 20 TPM, A’s additional jobs defer even if global headroom exists (strict guarantee model).
- If strict fairness is not required, set per-customer caps to 0 and rely on global caps + priority ordering.

### Unallocated headroom

Two pragmatic modes:

1. **Strict fairness**: per-customer caps enforce hard ceilings. Unused capacity remains unused (simple, predictable).
2. **Flexible** (recommended): set per-customer caps higher than allocation and enforce fairness via priority + throttling metrics; allow headroom borrowing.

In this implementation we support both via configuration.

---

## 6. Recording Pipeline Fix

We removed the fixed 45-second sleep and replaced it with:

- **Dedicated recording orchestration step** with states persisted in Redis (`RecordingStatus`) and job state in Postgres.
- **Bounded exponential backoff** retries without long-running sleeps.
- **Terminal states**: `AVAILABLE`, `TIMEOUT`, `FAILED` are explicit and logged.

Flow:

- Attempt fetch/upload once
- If not ready: reschedule `RECORDING_ORCHESTRATION` job using backoff (`next_run_at`)
- If available: enqueue `POSTCALL_ANALYSIS` job
- If attempts exhausted: dead-letter recording job with visible failure

Visibility:

- Each retry and terminal outcome logs structured events and metrics (`recording_retry_scheduled`, `recording_terminal_state`).

---

## 7. Reliability & Durability

### Source of truth

Postgres table `workflow_jobs` is authoritative:

- Job is never “lost” if Redis restarts
- Celery can fail to deliver messages; beat-driven drain will still pick due jobs

### Worker-safe concurrency

Workers claim jobs transactionally:

- `SELECT ... FOR UPDATE SKIP LOCKED`
- ordered by `priority DESC`, then `next_run_at ASC`

This prevents duplicates and supports many concurrent workers.

### Retries and dead-lettering

- Failures move jobs to `RETRY` and update `next_run_at` with exponential backoff.
- After `max_attempts`, jobs move to `DEAD_LETTER` with `last_error` preserved.

### Crash recovery

A sweeper periodically moves abandoned RUNNING jobs back to RETRY:

- if `locked_at < now - lock_timeout`

---

## 8. Auditability & Observability

### Structured logs (minimum fields)

Every job transition logs:

- `interaction_id`
- `job_id`
- `customer_id`
- `job_type`
- `old_status`, `new_status`
- `error`
- `timestamp`

Throttling decisions log:

- `allow_dispatch`, `delay_seconds`, `throttle_level`, `reason`
- `rpm`, `tpm`, `queue_depth`, `retry_depth`, `utilization_ratio`

### Metrics to expose (recommended)

- **Workflow**
  - queue depth by status (`PENDING`, `RETRY`, `RUNNING`, `DEAD_LETTER`)
  - claim rate, completion rate
  - retry counts, dead-letter counts
- **LLM**
  - global rpm/tpm utilization
  - per-customer rpm/tpm utilization
  - limiter denies / deferrals
  - provider 429 rate + retry-after durations
- **Recording**
  - success rate, timeout rate, attempts distribution

### Alert examples

- Dead-letter count > 0 in 5 minutes
- Recording TIMEOUT rate > X%
- Limiter denies > threshold (sustained pressure)
- Outage mode triggered (provider error spike)
- RUNNING jobs older than lock timeout repeatedly recovered

Debugging 3 days later:

- Query `workflow_jobs` by `interaction_id` to see status, attempts, errors.
- Use structured logs filtered by `job_id` / `interaction_id` to reconstruct timeline.

---

## 9. Data Model

### New table: `workflow_jobs`

```sql
CREATE TABLE workflow_jobs (
  id uuid PRIMARY KEY,
  interaction_id uuid NOT NULL,
  customer_id uuid NOT NULL,
  job_type text NOT NULL,
  status text NOT NULL,
  payload jsonb NOT NULL,
  priority int NOT NULL,
  attempts int NOT NULL,
  max_attempts int NOT NULL,
  next_run_at timestamptz NOT NULL,
  locked_by text NULL,
  locked_at timestamptz NULL,
  last_error text NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ON workflow_jobs (status, next_run_at);
CREATE INDEX ON workflow_jobs (priority, next_run_at);
CREATE INDEX ON workflow_jobs (interaction_id);
CREATE INDEX ON workflow_jobs (customer_id);
```

(Implementation uses SQLAlchemy model `WorkflowJob`.)

---

## 10. Security

Sensitive data:

- **Lead PII**: phone, name, email
- **Transcripts**: may contain PII and sensitive conversation content
- **Recordings**: raw audio is sensitive
- **LLM prompts/responses**: can contain PII and business data

Controls (pragmatic):

- **Transport security**: TLS for provider APIs, DB connections, internal services.
- **At-rest**:
  - Postgres disk encryption (managed service feature) or volume encryption
  - S3 bucket encryption (SSE-S3 or SSE-KMS)
- **Access control**:
  - least-privilege DB users
  - scoped IAM for S3
- **Logging hygiene**:
  - never log full transcripts/recordings
  - log IDs + hashes + counts, not raw content
- **Retention**:
  - TTL/retention policy for recordings and transcripts per customer contract

---

## 11. Trade-offs & Alternatives Considered

| Option | Why Considered | Why Rejected / What Chosen |
|--------|----------------|----------------------------|
| Keep Redis-only Celery | Minimal change | Still loses state on restart; Postgres job log needed |
| Kafka / Temporal | Strong durability + scheduling | Too heavy for scope; pragmatic Postgres job table used |
| Hard global FIFO queue | Simple | Starves high-value calls; priority + next_run_at required |
| Only RPM-based breaker | Easy | Providers rate-limit by tokens; TPM must be enforced |

---

## 12. Known Weaknesses

1. **Token estimation**: reservation uses `LLM_AVG_TOKENS_PER_CALL`. True tokens vary; production should refine estimate or use a two-step model.
2. **Retry-After parsing**: mock LLM client doesn’t provide headers; production needs explicit `Retry-After` handling in HTTP client.
3. **Schema migration**: SQLAlchemy model exists; production should include Alembic migrations and deployment sequencing.
4. **Downstream idempotency**: CRM pushes / notifications require dedupe keys and delivery logs for true safety.

---

## 13. What I Would Do With More Time

1. Add Alembic migrations + CI check for schema drift.
2. Add per-customer fairness beyond hard caps (weighted fair sharing across active customers).
3. Add a persistent audit/event table for immutable execution history (beyond logs).
4. Make LLM scheduling multi-queue (priority lanes) and add “urgent classification” pre-step.
5. Add dashboards + alerts (Grafana/Prometheus) for limiter, backlog, dead-letter.

