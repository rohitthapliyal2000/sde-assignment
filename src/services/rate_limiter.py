from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

from src.config import settings
from src.utils.redis_client import redis_client

logger = logging.getLogger(__name__)


RESERVE_LUA = r"""
-- KEYS:
-- 1) global_rpm_key
-- 2) global_tpm_key
-- 3) customer_rpm_key
-- 4) customer_tpm_key
--
-- ARGV:
-- 1) req_units (usually 1)
-- 2) token_units (estimated tokens)
-- 3) global_rpm_limit
-- 4) global_tpm_limit
-- 5) customer_rpm_limit (0 disables)
-- 6) customer_tpm_limit (0 disables)
-- 7) window_seconds

local req_units = tonumber(ARGV[1])
local tok_units = tonumber(ARGV[2])
local g_rpm_limit = tonumber(ARGV[3])
local g_tpm_limit = tonumber(ARGV[4])
local c_rpm_limit = tonumber(ARGV[5])
local c_tpm_limit = tonumber(ARGV[6])
local window = tonumber(ARGV[7])

local function ttl_or_window(key)
  local t = redis.call("TTL", key)
  if t == -2 then return window end
  if t == -1 then return window end
  return t
end

local g_rpm = tonumber(redis.call("GET", KEYS[1]) or "0")
local g_tpm = tonumber(redis.call("GET", KEYS[2]) or "0")
local c_rpm = tonumber(redis.call("GET", KEYS[3]) or "0")
local c_tpm = tonumber(redis.call("GET", KEYS[4]) or "0")

local allowed = 1
if g_rpm + req_units > g_rpm_limit then allowed = 0 end
if g_tpm + tok_units > g_tpm_limit then allowed = 0 end
if c_rpm_limit > 0 and (c_rpm + req_units > c_rpm_limit) then allowed = 0 end
if c_tpm_limit > 0 and (c_tpm + tok_units > c_tpm_limit) then allowed = 0 end

if allowed == 0 then
  local retry_after = math.max(
    ttl_or_window(KEYS[1]),
    ttl_or_window(KEYS[2]),
    ttl_or_window(KEYS[3]),
    ttl_or_window(KEYS[4])
  )
  return {0, retry_after, g_rpm, g_tpm, c_rpm, c_tpm}
end

local new_g_rpm = redis.call("INCRBY", KEYS[1], req_units)
local new_g_tpm = redis.call("INCRBY", KEYS[2], tok_units)
redis.call("EXPIRE", KEYS[1], window)
redis.call("EXPIRE", KEYS[2], window)

local new_c_rpm = redis.call("INCRBY", KEYS[3], req_units)
local new_c_tpm = redis.call("INCRBY", KEYS[4], tok_units)
redis.call("EXPIRE", KEYS[3], window)
redis.call("EXPIRE", KEYS[4], window)

return {1, 0, new_g_rpm, new_g_tpm, new_c_rpm, new_c_tpm}
"""


@dataclass(frozen=True)
class ReservationDecision:
    allowed: bool
    retry_after_seconds: int
    global_rpm: int
    global_tpm: int
    customer_rpm: int
    customer_tpm: int


class DeferredDueToRateLimit(Exception):
    def __init__(self, retry_after_seconds: int, reason: str):
        super().__init__(reason)
        self.retry_after_seconds = retry_after_seconds
        self.reason = reason


class ProviderRateLimitError(Exception):
    def __init__(self, retry_after_seconds: int, message: str = "provider_429"):
        super().__init__(message)
        self.retry_after_seconds = retry_after_seconds


class RedisRateLimiter:
    def __init__(self, window_seconds: int = 60):
        self.window_seconds = window_seconds

    async def reserve(
        self,
        *,
        customer_id: str,
        requests: int = 1,
        tokens: int,
    ) -> ReservationDecision:
        keys = [
            "llm:limit:global:rpm",
            "llm:limit:global:tpm",
            f"llm:limit:customer:{customer_id}:rpm",
            f"llm:limit:customer:{customer_id}:tpm",
        ]
        args = [
            int(requests),
            int(tokens),
            int(settings.LLM_REQUESTS_PER_MINUTE),
            int(settings.LLM_TOKENS_PER_MINUTE),
            int(settings.LLM_PER_CUSTOMER_REQUESTS_PER_MINUTE),
            int(settings.LLM_PER_CUSTOMER_TOKENS_PER_MINUTE),
            int(self.window_seconds),
        ]
        allowed, retry_after, g_rpm, g_tpm, c_rpm, c_tpm = await redis_client.eval(
            RESERVE_LUA, len(keys), *keys, *args
        )
        return ReservationDecision(
            allowed=bool(int(allowed)),
            retry_after_seconds=int(retry_after),
            global_rpm=int(g_rpm),
            global_tpm=int(g_tpm),
            customer_rpm=int(c_rpm),
            customer_tpm=int(c_tpm),
        )


rate_limiter = RedisRateLimiter()
