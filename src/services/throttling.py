from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Optional

from src.config import settings


@dataclass(frozen=True)
class ThrottleDecision:
    allow_dispatch: bool
    throttle_level: float  # 0.0 (none) -> 1.0 (max)
    delay_seconds: int
    reason: str
    utilization_ratio: float
    queue_depth: int
    retry_depth: int
    rpm: int
    tpm: int
    outage_mode: bool = False


def clamp01(x: float) -> float:
    return max(0.0, min(1.0, x))


def compute_throttle_level(
    *,
    utilization_ratio: float,
    queue_depth: int,
    retry_depth: int,
) -> float:
    """
    Convert multiple pressure signals into a single [0,1] throttle level.
    """
    soft_u = settings.THROTTLE_SOFT_UTILIZATION
    hard_u = settings.THROTTLE_HARD_UTILIZATION
    if hard_u <= soft_u:
        hard_u = soft_u + 1e-6

    util_pressure = clamp01((utilization_ratio - soft_u) / (hard_u - soft_u))
    queue_pressure = clamp01(queue_depth / max(1, settings.THROTTLE_QUEUE_SOFT_LIMIT))
    retry_pressure = clamp01(retry_depth / max(1, settings.THROTTLE_RETRY_SOFT_LIMIT))

    return clamp01(max(util_pressure, queue_pressure, retry_pressure))


def compute_delay_seconds(level: float) -> int:
    """
    Convert throttle level into a small scheduling delay.
    """
    if level <= 0:
        return 0
    # Mild curve: small delays early, steeper near saturation.
    delay = int(math.ceil((level**1.5) * settings.THROTTLE_MAX_DELAY_SECONDS))
    return min(settings.THROTTLE_MAX_DELAY_SECONDS, max(1, delay))


def decide_dispatch(
    *,
    priority: int,
    utilization_ratio: float,
    queue_depth: int,
    retry_depth: int,
    rpm: int,
    tpm: int,
    outage_mode: bool,
) -> ThrottleDecision:
    """
    Decide whether to dispatch now, delay, or defer based on pressure.

    Priority convention:
    - Higher number means higher priority (aligns with `WorkflowJob.priority`).
    """
    level = compute_throttle_level(
        utilization_ratio=utilization_ratio,
        queue_depth=queue_depth,
        retry_depth=retry_depth,
    )
    delay = compute_delay_seconds(level)
    is_high = priority >= settings.THROTTLE_HIGH_PRIORITY_MIN

    if outage_mode:
        # Only catastrophic failure uses a real "stop" signal.
        return ThrottleDecision(
            allow_dispatch=False,
            throttle_level=1.0,
            delay_seconds=settings.THROTTLE_OUTAGE_FREEZE_SECONDS,
            reason="outage_mode",
            utilization_ratio=utilization_ratio,
            queue_depth=queue_depth,
            retry_depth=retry_depth,
            rpm=rpm,
            tpm=tpm,
            outage_mode=True,
        )

    # Under hard pressure, defer low priority but keep high priority flowing with delay.
    if utilization_ratio >= settings.THROTTLE_HARD_UTILIZATION and not is_high:
        return ThrottleDecision(
            allow_dispatch=False,
            throttle_level=level,
            delay_seconds=delay,
            reason="hard_utilization_defer_low_priority",
            utilization_ratio=utilization_ratio,
            queue_depth=queue_depth,
            retry_depth=retry_depth,
            rpm=rpm,
            tpm=tpm,
        )

    # Default: allow, possibly with a small delay recommendation.
    return ThrottleDecision(
        allow_dispatch=True,
        throttle_level=level,
        delay_seconds=delay,
        reason="graded_throttle",
        utilization_ratio=utilization_ratio,
        queue_depth=queue_depth,
        retry_depth=retry_depth,
        rpm=rpm,
        tpm=tpm,
    )
