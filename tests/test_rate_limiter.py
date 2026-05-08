import pytest

from src.services.rate_limiter import ReservationDecision, DeferredDueToRateLimit
from datetime import datetime, timezone


@pytest.mark.asyncio
async def test_defer_on_insufficient_budget(monkeypatch):
    from src.services import post_call_processor as pcp

    async def fake_reserve(*, customer_id, requests, tokens):
        return ReservationDecision(
            allowed=False,
            retry_after_seconds=12,
            global_rpm=500,
            global_tpm=90000,
            customer_rpm=0,
            customer_tpm=0,
        )

    monkeypatch.setattr(pcp.rate_limiter, "reserve", fake_reserve)

    ctx = pcp.PostCallContext(
        interaction_id="i1",
        session_id="s1",
        lead_id="l1",
        campaign_id="c1",
        customer_id="cust1",
        agent_id="a1",
        call_sid="call1",
        transcript_text="agent: hi",
        conversation_data={},
        additional_data={},
        ended_at=datetime.now(timezone.utc),
        exotel_account_id=None,
    )

    processor = pcp.PostCallProcessor()
    with pytest.raises(DeferredDueToRateLimit) as e:
        await processor.process_post_call(ctx)
    assert e.value.retry_after_seconds == 12

