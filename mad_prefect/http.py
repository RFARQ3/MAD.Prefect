"""HTTPX read requests with library retries and optional shared pacing."""

import asyncio
import logging
import math
from typing import TYPE_CHECKING, Any

import httpx
from httpx_retries import Retry, aretry_request
from prefect.exceptions import MissingContextError
from prefect.logging import get_run_logger
from pydantic import BaseModel, ConfigDict, Field

if TYPE_CHECKING:
    from mad_prefect.rate_limit import ApiRateLimit


class HttpRetryPolicy(BaseModel):
    model_config = ConfigDict(frozen=True)

    max_attempts: int = Field(default=3, ge=1, le=100)
    total_timeout_seconds: float = Field(default=120, gt=0, allow_inf_nan=False)
    backoff_factor: float = Field(default=0.5, ge=0, allow_inf_nan=False)
    max_backoff_seconds: float = Field(default=30, gt=0, allow_inf_nan=False)
    backoff_jitter: float = Field(default=1, ge=0, le=1)
    retry_statuses: tuple[int, ...] = (429, 500, 502, 503, 504)
    rate_limit_fallback_seconds: float | None = Field(default=None, ge=0, allow_inf_nan=False)


class _LimiterFailure(Exception):
    """Keep exhausted limiter errors outside the HTTP retry policy."""


class RetryingAsyncClient(httpx.AsyncClient):
    """Use get/head for buffered reads; retries and pacing are coordinated here."""

    def __init__(self, *, retry: HttpRetryPolicy | None = None,
                 rate_limit: 'ApiRateLimit | None' = None, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.retry_policy = retry or HttpRetryPolicy()
        self.rate_limit = rate_limit
        self.event_hooks['request'].append(self._acquire_allowance)

    async def _acquire_allowance(self, request: httpx.Request) -> None:
        if self.rate_limit is not None:
            try:
                await self.rate_limit.acquire()
            except Exception as error:
                # A second HTTP retry loop must not multiply limiter retries.
                raise _LimiterFailure() from error

    async def request(self, method: str, url: httpx.URL | str, **kwargs: Any) -> httpx.Response:
        if method.upper() not in ('GET', 'HEAD'):
            raise ValueError('RetryingAsyncClient supports GET and HEAD only')
        if 'retry' in (kwargs.get('extensions') or {}):
            raise ValueError('Configure retries through HttpRetryPolicy, not request extensions')
        try:
            logger = get_run_logger()
        except MissingContextError:
            logger = logging.getLogger(__name__)
        policy = self.retry_policy
        deadline = asyncio.get_running_loop().time() + policy.total_timeout_seconds

        # The library owns the retry loop, exception selection and response cleanup.
        # Override only waiting: provider Retry-After must never be clipped early.
        class RequestRetry(Retry):
            async def asleep(self, response: httpx.Response | Exception) -> None:
                delay = None
                if isinstance(response, httpx.Response):
                    header = response.headers.get('Retry-After')
                    if header:
                        try:
                            parsed = self.parse_retry_after(header)
                            if math.isfinite(parsed) and parsed >= 0:
                                delay = parsed
                        except (ValueError, OverflowError):
                            pass
                    if delay is None and response.status_code == 429:
                        delay = policy.rate_limit_fallback_seconds
                if delay is None:
                    delay = self.backoff_strategy()
                if delay >= deadline - asyncio.get_running_loop().time():
                    raise TimeoutError('HTTP retry delay exceeds the remaining request budget')
                reason = (f'HTTP {response.status_code}' if isinstance(response, httpx.Response)
                          else type(response).__name__)
                logger.warning('HTTP %s retry %d/%d after %s; waiting %.2fs',
                               method.upper(), self.attempts_made, self.total, reason, delay)
                await asyncio.sleep(delay)

        retry = RequestRetry(
            total=policy.max_attempts - 1, allowed_methods=('GET', 'HEAD'),
            status_forcelist=policy.retry_statuses,
            backoff_factor=policy.backoff_factor, max_backoff_wait=policy.max_backoff_seconds,
            backoff_jitter=policy.backoff_jitter,
        )
        # An empty list otherwise selects the library defaults rather than no statuses.
        retry.status_forcelist = frozenset(policy.retry_statuses)
        try:
            async with asyncio.timeout(policy.total_timeout_seconds):
                response = await aretry_request(self, method, url, retry=retry, **kwargs)
        except _LimiterFailure as error:
            assert error.__cause__ is not None
            raise error.__cause__ from None
        except Exception as error:
            logger.error('HTTP %s stopped (%s)', method.upper(), type(error).__name__)
            raise
        if response.is_error:
            logger.error('HTTP %s finished with status %d', method.upper(), response.status_code)
        elif response.extensions.get('retry', retry).attempts_made:
            logger.info('HTTP %s recovered after retry', method.upper())
        return response
