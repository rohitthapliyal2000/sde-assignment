import pytest

from src.services.throttling import decide_dispatch


def test_low_priority_deferred_at_hard_utilization():
    d = decide_dispatch(
        priority=10,
        utilization_ratio=1.1,
        queue_depth=0,
        retry_depth=0,
        rpm=600,
        tpm=0,
        outage_mode=False,
    )
    assert d.allow_dispatch is False
    assert d.delay_seconds >= 1


def test_high_priority_allowed_under_pressure_with_delay():
    d = decide_dispatch(
        priority=1000,
        utilization_ratio=1.1,
        queue_depth=10_000,
        retry_depth=2_000,
        rpm=600,
        tpm=0,
        outage_mode=False,
    )
    assert d.allow_dispatch is True
    assert d.delay_seconds >= 1
    assert d.throttle_level > 0


def test_outage_mode_blocks_everything_short_term():
    d = decide_dispatch(
        priority=1000,
        utilization_ratio=0.2,
        queue_depth=0,
        retry_depth=0,
        rpm=1,
        tpm=1,
        outage_mode=True,
    )
    assert d.allow_dispatch is False
    assert d.outage_mode is True
