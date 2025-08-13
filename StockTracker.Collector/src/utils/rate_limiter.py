"""Rate limiting utilities with flexible configuration support."""

import asyncio
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, Optional, Union

import structlog

logger = structlog.get_logger()


@dataclass
class RateLimitRule:
    """Configuration for a specific rate limit rule."""

    limit: int
    period: int
    burst_limit: Optional[int] = None
    burst_period: Optional[int] = None

    def __post_init__(self):
        """Validate rule after initialization."""
        if self.limit <= 0:
            raise ValueError("Rate limit must be positive")
        if self.period <= 0:
            raise ValueError("Rate limit period must be positive")
        if self.burst_limit and self.burst_limit <= 0:
            raise ValueError("Burst limit must be positive")
        if self.burst_period and self.burst_period <= 0:
            raise ValueError("Burst period must be positive")


@dataclass
class RequestRecord:
    """Record of a single request for rate limiting."""

    timestamp: float
    count: int = 1


class TokenBucket:
    """Token bucket implementation for rate limiting."""

    def __init__(self, limit: int, period: int):
        """
        Initialize token bucket.

        Args:
            limit: Maximum number of tokens
            period: Time period in seconds to refill tokens
        """
        self.limit = limit
        self.period = period
        self.tokens = limit
        self.last_refill = time.time()
        self._lock = asyncio.Lock()

    async def consume(self, tokens: int = 1) -> bool:
        """
        Try to consume tokens from bucket.

        Args:
            tokens: Number of tokens to consume

        Returns:
            True if tokens were consumed, False if not enough tokens
        """
        async with self._lock:
            now = time.time()
            elapsed = now - self.last_refill
            tokens_to_add = (elapsed / self.period) * self.limit
            self.tokens = min(self.limit, self.tokens + tokens_to_add)
            self.last_refill = now

            if self.tokens >= tokens:
                self.tokens -= tokens
                return True
            return False

    def get_wait_time(self, tokens: int = 1) -> float:
        """Get time to wait before tokens are available."""
        if self.tokens >= tokens:
            return 0.0

        tokens_needed = tokens - self.tokens
        return (tokens_needed / self.limit) * self.period


class SlidingWindowRateLimiter:
    """Sliding window rate limiter implementation."""

    def __init__(self, rule: RateLimitRule):
        """Initialize with rate limit rule."""
        self.rule = rule
        self.requests: Dict[str, list] = defaultdict(list)
        self._locks: Dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

    async def is_allowed(self, key: str) -> bool:
        """
        Check if request is allowed for given key.

        Args:
            key: Identifier for rate limit bucket (e.g., API endpoint, user ID)

        Returns:
            True if request is allowed, False otherwise
        """
        async with self._locks[key]:
            now = time.time()
            window_start = now - self.rule.period

            self.requests[key] = [req for req in self.requests[key] if req >= window_start]

            if len(self.requests[key]) < self.rule.limit:
                self.requests[key].append(now)
                return True

            return False

    async def get_wait_time(self, key: str) -> float:
        """Get time to wait before next request is allowed."""
        async with self._locks[key]:
            now = time.time()
            window_start = now - self.rule.period

            self.requests[key] = [req for req in self.requests[key] if req >= window_start]

            if len(self.requests[key]) < self.rule.limit:
                return 0.0

            oldest_request = min(self.requests[key])
            return oldest_request + self.rule.period - now

    def get_remaining_requests(self, key: str) -> int:
        """Get number of remaining requests in current window."""
        now = time.time()
        window_start = now - self.rule.period

        self.requests[key] = [req for req in self.requests[key] if req >= window_start]

        return max(0, self.rule.limit - len(self.requests[key]))


class AdaptiveRateLimiter:
    """Adaptive rate limiter that can adjust limits based on response times."""

    def __init__(self, base_rule: RateLimitRule, adaptation_factor: float = 0.1):
        """
        Initialize adaptive rate limiter.

        Args:
            base_rule: Base rate limit rule
            adaptation_factor: How much to adjust limits (0.0 to 1.0)
        """
        self.base_rule = base_rule
        self.adaptation_factor = adaptation_factor
        self.current_limit = base_rule.limit
        self.response_times: list = []
        self.base_limiter = SlidingWindowRateLimiter(base_rule)
        self._last_adjustment = time.time()

    async def is_allowed(self, key: str) -> bool:
        """Check if request is allowed with adaptive limits."""
        adaptive_rule = RateLimitRule(
            limit=int(self.current_limit),
            period=self.base_rule.period,
            burst_limit=self.base_rule.burst_limit,
            burst_period=self.base_rule.burst_period,
        )

        self.base_limiter.rule = adaptive_rule
        return await self.base_limiter.is_allowed(key)

    def record_response_time(self, response_time: float):
        """Record response time for adaptive adjustment."""
        self.response_times.append(response_time)

        if len(self.response_times) > 100:
            self.response_times = self.response_times[-100:]

        now = time.time()
        if now - self._last_adjustment > 60:
            self._adjust_limits()
            self._last_adjustment = now

    def _adjust_limits(self):
        """Adjust rate limits based on recent performance."""
        if len(self.response_times) < 10:
            return

        avg_response_time = sum(self.response_times) / len(self.response_times)

        if avg_response_time < 1.0:
            adjustment = 1 + self.adaptation_factor
        elif avg_response_time > 5.0:
            adjustment = 1 - self.adaptation_factor
        else:
            adjustment = 1.0

        new_limit = self.current_limit * adjustment

        min_limit = self.base_rule.limit * 0.1
        max_limit = self.base_rule.limit * 2.0

        self.current_limit = max(min_limit, min(max_limit, new_limit))

        logger.info(
            "Adjusted rate limit",
            base_limit=self.base_rule.limit,
            current_limit=int(self.current_limit),
            avg_response_time=avg_response_time,
        )


class MultiTierRateLimiter:
    """Multi-tier rate limiter supporting different limits for different operations."""

    def __init__(self):
        """Initialize multi-tier rate limiter."""
        self.limiters: Dict[str, Union[SlidingWindowRateLimiter, AdaptiveRateLimiter]] = {}
        self.default_rule = RateLimitRule(limit=100, period=60)

    def add_rule(self, name: str, rule: RateLimitRule, adaptive: bool = False):
        """
        Add a rate limit rule.

        Args:
            name: Name of the rule (e.g., 'api_calls', 'database_queries')
            rule: Rate limit rule configuration
            adaptive: Whether to use adaptive rate limiting
        """
        if adaptive:
            self.limiters[name] = AdaptiveRateLimiter(rule)
        else:
            self.limiters[name] = SlidingWindowRateLimiter(rule)

    async def is_allowed(self, rule_name: str, key: str) -> bool:
        """
        Check if request is allowed for specific rule and key.

        Args:
            rule_name: Name of the rate limit rule
            key: Identifier for rate limit bucket

        Returns:
            True if allowed, False otherwise
        """
        if rule_name not in self.limiters:
            self.limiters[rule_name] = SlidingWindowRateLimiter(self.default_rule)

        return await self.limiters[rule_name].is_allowed(key)

    async def wait_if_needed(self, rule_name: str, key: str, max_wait: float = 60.0):
        """
        Wait if rate limit is exceeded.

        Args:
            rule_name: Name of the rate limit rule
            key: Identifier for rate limit bucket
            max_wait: Maximum time to wait in seconds
        """
        if rule_name not in self.limiters:
            return

        limiter = self.limiters[rule_name]
        if hasattr(limiter, "get_wait_time"):
            wait_time = await limiter.get_wait_time(key)
            if wait_time > 0 and wait_time <= max_wait:
                logger.info("Rate limit hit, waiting", wait_time=wait_time, rule=rule_name, key=key)
                await asyncio.sleep(wait_time)

    def record_response_time(self, rule_name: str, response_time: float):
        """Record response time for adaptive limiters."""
        if rule_name in self.limiters:
            limiter = self.limiters[rule_name]
            if isinstance(limiter, AdaptiveRateLimiter):
                limiter.record_response_time(response_time)


rate_limiter = MultiTierRateLimiter()
