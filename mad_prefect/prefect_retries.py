"""Retry temporary Prefect failures when acquiring a rate-limit allowance."""

from __future__ import annotations

import asyncio
import logging
import random
from collections.abc import Awaitable, Callable, Coroutine
from functools import wraps
from typing import TYPE_CHECKING, Any

from httpx import HTTPStatusError, NetworkError, RemoteProtocolError, TimeoutException
from prefect.concurrency._asyncio import ConcurrencySlotAcquisitionError
from prefect.exceptions import MissingContextError
from prefect.logging import get_run_logger

if TYPE_CHECKING:
    from mad_prefect.rate_limit import ApiRateLimit


def retry_prefect_acquisition(
    acquire: Callable[[ApiRateLimit], Awaitable[None]],
) -> Callable[[ApiRateLimit], Coroutine[Any, Any, None]]:
    """Retry the decorated allowance-acquisition method after temporary failures.

    Use the limiter's max_attempts, including the first call. Wait between
    retries with increasing jittered delays, and log failure, recovery or
    exhaustion. Raise the original error when retrying cannot help or attempts
    run out. Cancellation propagates immediately.

    This decorator creates no Prefect task and never retries downstream API
    requests. Attempt counters and delays belong to each individual call.
    """
    @wraps(acquire)
    async def acquire_with_retries(limiter: ApiRateLimit) -> None:
        """Call the acquisition method again only after a retryable Prefect failure."""
        # Attach recovery messages to the flow when available. Standalone callers
        # still get the same messages through normal Python logging.
        try:
            logger = get_run_logger()
        except MissingContextError:
            logger = logging.getLogger(__name__)

        retry_delay_seconds = 1.0
        for attempt in range(1, limiter.max_attempts + 1):
            try:
                await acquire(limiter)
            except Exception as error:
                # Log error types, not messages that might contain credentials.
                error_description = type(error).__name__
                if error.__cause__ is not None:
                    error_description += f" caused by {type(error.__cause__).__name__}"

                if not is_retryable_prefect_error(error) or attempt == limiter.max_attempts:
                    logger.error(
                        "Rate limit %s failed on attempt %d/%d (%s); no request permitted",
                        limiter.name, attempt, limiter.max_attempts, error_description,
                    )
                    raise

                # Spread out reconnecting pods. Backoff starts at one second,
                # doubles after each failure and stops growing at ten seconds.
                wait_seconds = random.uniform(retry_delay_seconds / 2, retry_delay_seconds)
                logger.warning(
                    "Rate limit %s attempt %d/%d failed (%s); retrying in %.2fs",
                    limiter.name, attempt, limiter.max_attempts, error_description, wait_seconds,
                )
                await asyncio.sleep(wait_seconds)
                retry_delay_seconds = min(retry_delay_seconds * 2, 10)
            else:
                if attempt > 1:
                    logger.info("Rate limit %s recovered on attempt %d/%d",
                                limiter.name, attempt, limiter.max_attempts)
                return

    return acquire_with_retries


def is_retryable_prefect_error(error: Exception) -> bool:
    """Check whether a failed request to Prefect is worth retrying.

    Return True for network failures, timeouts, invalid HTTP responses,
    or HTTP status 500, 502, 503 or 504. These failures may be temporary.

    Return False for other errors, including missing limits, invalid settings
    and authentication failures.

    Inspect the original cause when Prefect wraps an error during rate-limit
    acquisition. This function only checks the error; it does not retry anything
    or handle requests to the downstream API.
    """
    # Prefect wraps both connection failures and missing-limit errors.
    # Inspect the original cause to distinguish between them. Repeating this
    # also handles nested wrappers without treating the wrapper as the cause.
    while isinstance(error, ConcurrencySlotAcquisitionError):
        original_error = error.__cause__

        if not isinstance(original_error, Exception):
            return False

        if original_error is error:
            return False

        error = original_error

    # These failures may resolve when we contact Prefect again.
    temporary_connection_errors = (
        NetworkError,
        TimeoutException,
        RemoteProtocolError,
    )
    if isinstance(error, temporary_connection_errors):
        return True

    # Only these server responses qualify for another attempt.
    if isinstance(error, HTTPStatusError):
        retryable_status_codes = (500, 502, 503, 504)
        return error.response.status_code in retryable_status_codes

    return False
