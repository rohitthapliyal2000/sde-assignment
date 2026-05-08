"""
Graded backpressure + catastrophic breaker.

This replaces the old binary "freeze for 1800s at 90% RPM" behavior.

- Under load: we *throttle proportionally* (recommend small delays and defer low
  priority) based on utilization and backlog signals.
- Under outage: we keep a *short-lived circuit breaker* that returns "do not
  dispatch" for a brief window when provider errors indicate a real dependency
  failure.
"""

import logging
import time
from dataclasses import dataclass
from typing import Optional

from src.config import settings
from src.services.throttling import ThrottleDecision, decide_dispatch
from src.utils.redis_client import redis_client

logger = logging.getLogger(__name__)


@dataclass
class OutageState:
    opened_at: Optional[float] = None
    freeze_until: Optional[float] = None


class PostCallCircuitBreaker:
    def __init__(self):
        self._outage = OutageState()

    async def check_dispatch(
        self,
        *,
        agent_id: str,
        interaction_id: Optional[str] = None,
        customer_id: Optional[str] = None,
        job_type: Optional[str] = None,
        priority: int = 100,
    ) -> ThrottleDecision:
        now = time.time()

        outage_mode = False
        if self._outage.freeze_until and now < self._outage.freeze_until:
            outage_mode = True

        rpm = int(await redis_client.get("llm:postcall:rpm") or 0)
        tpm = int(await redis_client.get("llm:postcall:tpm") or 0)
        max_rpm = max(1, settings.LLM_REQUESTS_PER_MINUTE)
        max_tpm = max(1, settings.LLM_TOKENS_PER_MINUTE)
        util = max(rpm / max_rpm, tpm / max_tpm)

        queue_depth = int(await redis_client.get("workflow:queue_depth") or 0)
        retry_depth = int(await redis_client.get("workflow:retry_depth") or 0)

        decision = decide_dispatch(
            priority=priority,
            utilization_ratio=util,
            queue_depth=queue_depth,
            retry_depth=retry_depth,
            rpm=rpm,
            tpm=tpm,
            outage_mode=outage_mode,
        )

        logger.info(
            "throttle_decision",
            extra={
                "agent_id": agent_id,
                "interaction_id": interaction_id,
                "customer_id": customer_id,
                "job_type": job_type,
                "priority": priority,
                "allow_dispatch": decision.allow_dispatch,
                "delay_seconds": decision.delay_seconds,
                "throttle_level": round(decision.throttle_level, 3),
                "reason": decision.reason,
                "utilization_ratio": round(decision.utilization_ratio, 3),
                "queue_depth": decision.queue_depth,
                "retry_depth": decision.retry_depth,
                "rpm": decision.rpm,
                "tpm": decision.tpm,
                "outage_mode": decision.outage_mode,
            },
        )
        return decision

    async def check_capacity(self, agent_id: str) -> bool:
        """
        Backward-compatible boolean gate for dialer integration.
        """
        decision = await self.check_dispatch(agent_id=agent_id, priority=0)
        return decision.allow_dispatch

    async def record_postcall_start(self):
        """
        Increment the RPM counter when a post-call LLM request starts.

        This runs AFTER we've decided to fire the request, so it's a
        measurement, not a gate. The dialler reads this counter to make
        dispatch decisions — there's a lag between when requests go out
        and when the counter updates.
        """
        await redis_client.incr("llm:postcall:rpm")
        await redis_client.expire("llm:postcall:rpm", 60)

    async def record_postcall_end(self):
        """
        Decrement the RPM counter when the LLM request completes.

        If the worker crashes between start and end, the counter stays
        inflated until the 60-second TTL expires. During that window,
        the circuit breaker may trip unnecessarily.
        """
        await redis_client.decr("llm:postcall:rpm")

    async def record_provider_error(self, error_code: str = "unknown"):
        """
        Track provider-side errors for short-lived outage detection.
        """
        await redis_client.incr("llm:postcall:errors")
        await redis_client.expire("llm:postcall:errors", 60)
        logger.warning("llm_provider_error", extra={"error_code": error_code})

        errors = int(await redis_client.get("llm:postcall:errors") or 0)
        if errors >= settings.THROTTLE_OUTAGE_ERROR_THRESHOLD:
            now = time.time()
            self._outage.opened_at = now
            self._outage.freeze_until = now + settings.THROTTLE_OUTAGE_FREEZE_SECONDS
            logger.error(
                "llm_outage_mode_enabled",
                extra={
                    "errors_last_60s": errors,
                    "freeze_seconds": settings.THROTTLE_OUTAGE_FREEZE_SECONDS,
                },
            )


circuit_breaker = PostCallCircuitBreaker()
