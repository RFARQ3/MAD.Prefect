"""Shared request pacing through Prefect's global concurrency limits."""

import asyncio
import logging
import math
import random

from httpx import (
    AsyncBaseTransport,
    HTTPStatusError,
    NetworkError,
    RemoteProtocolError,
    Request,
    Response,
    TimeoutException,
)
from prefect.client.orchestration import get_client
from prefect.client.schemas.actions import GlobalConcurrencyLimitCreate
from prefect.concurrency.asyncio import rate_limit
from prefect.concurrency._asyncio import ConcurrencySlotAcquisitionError
from prefect.exceptions import MissingContextError, ObjectNotFound
from prefect.logging import get_run_logger
from prefect.settings import PREFECT_API_URL
from pydantic import BaseModel, ConfigDict, Field, PrivateAttr


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


class ApiRateLimit(BaseModel):
    """Share request spacing between processes using the same Prefect server.

    Use one non-secret name per provider account and reuse this instance across
    requests. Only successful setup is cached; each acquire still asks Prefect
    for a new allowance. HTTP retries and provider cooldowns belong to the caller.
    """

    model_config = ConfigDict(frozen=True)

    name: str = Field(min_length=1, pattern=r"^[a-zA-Z0-9][a-zA-Z0-9_-]*$")
    requests_per_second: float = Field(gt=0, allow_inf_nan=False)
    wait_timeout_seconds: float = Field(default=120, gt=0, allow_inf_nan=False)

    # These retries contact Prefect only. They never resend a provider request.
    max_attempts: int = Field(default=4, ge=1, le=100)
    retry_delay_seconds: float = Field(default=1, gt=0, allow_inf_nan=False)
    max_retry_delay_seconds: float = Field(default=10, gt=0, allow_inf_nan=False)

    _configuration_checked: bool = PrivateAttr(default=False)
    _setup_lock: asyncio.Lock = PrivateAttr(default_factory=asyncio.Lock)

    async def acquire(self) -> None:
        """Wait for one confirmed allowance, or fail without permitting a request.

        Temporary Prefect failures retry within one bounded deadline. No Prefect
        task is created, and callers do not need to release a decaying allowance.
        """
        if not PREFECT_API_URL.value():
            raise RuntimeError("Shared API rate limits require a configured PREFECT_API_URL")
        # Attach recovery messages to the flow when available. Standalone callers
        # still get the same messages through normal Python logging.
        try:
            logger = get_run_logger()
        except MissingContextError:
            logger = logging.getLogger(__name__)

        retry_delay_seconds = min(self.retry_delay_seconds, self.max_retry_delay_seconds)
        attempt = 0
        try:
            # One deadline covers setup, all attempts, backoff and slot waiting.
            async with asyncio.timeout(self.wait_timeout_seconds):
                for attempt in range(1, self.max_attempts + 1):
                    try:
                        await self._acquire_prefect_allowance()
                    except Exception as error:
                        # Keep the failure and its cause visible without logging
                        # exception messages, which can contain URLs or credentials.
                        error_description = type(error).__name__
                        if error.__cause__ is not None:
                            error_description += f" caused by {type(error.__cause__).__name__}"

                        if not is_retryable_prefect_error(error) or attempt == self.max_attempts:
                            logger.error(
                                "Rate limit %s failed on attempt %d/%d (%s); no request permitted",
                                self.name, attempt, self.max_attempts,
                                error_description,
                            )
                            raise

                        # Jitter keeps recovering pods from reconnecting together.
                        wait_seconds = random.uniform(retry_delay_seconds / 2, retry_delay_seconds)
                        logger.warning(
                            "Rate limit %s attempt %d/%d failed (%s); retrying in %.2fs",
                            self.name, attempt, self.max_attempts, error_description, wait_seconds,
                        )
                        await asyncio.sleep(wait_seconds)
                        retry_delay_seconds = min(
                            retry_delay_seconds * 2, self.max_retry_delay_seconds
                        )
                    else:
                        if attempt > 1:
                            logger.info("Rate limit %s recovered on attempt %d/%d",
                                        self.name, attempt, self.max_attempts)
                        return
        except TimeoutError:
            logger.error("Rate limit %s timed out on attempt %d/%d within %.2fs; no request permitted",
                         self.name, attempt, self.max_attempts, self.wait_timeout_seconds)
            raise

    async def _acquire_prefect_allowance(self) -> None:
        """Check the shared configuration once, then ask Prefect for one allowance.

        Return None only after Prefect grants permission. Raise on failed setup
        or acquisition so acquire() can apply its retry policy. Concurrent first
        calls share the setup lock, avoiding duplicate configuration reads.
        """
        if not self._configuration_checked:
            # Concurrent callers share setup; failures leave it eligible for retry.
            async with self._setup_lock:
                if not self._configuration_checked:
                    await self._create_or_check_prefect_limit()
                    self._configuration_checked = True

        # A successful allowance is consumed, not released. Prefect restores it
        # at the configured rate, which spaces requests across all participating pods.
        await rate_limit(
            self.name, occupy=1, strict=True,
            timeout_seconds=self.wait_timeout_seconds,
        )

    async def _create_or_check_prefect_limit(self) -> None:
        """Read the named Prefect limit, creating it if it does not exist.

        Require an active limit with capacity one and the requested decay rate.
        Return None if those settings match; raise ValueError if they differ.
        Never change an existing limit because other deployments may be using it.
        """
        # This client talks to the Prefect server, not the downstream API. Setup
        # happens once per limiter so ordinary requests only acquire an allowance.
        async with get_client() as client:
            try:
                limit = await client.read_global_concurrency_limit_by_name(self.name)
            except ObjectNotFound:
                # Capacity one prevents an initial burst. Decay controls how soon
                # another pod can consume the next allowance.
                try:
                    await client.create_global_concurrency_limit(
                        GlobalConcurrencyLimitCreate(
                            name=self.name,
                            limit=1,
                            active=True,
                            slot_decay_per_second=self.requests_per_second,
                        )
                    )
                except HTTPStatusError as error:
                    # Another pod can create the account's limit at the same time.
                    if error.response.status_code != 409:
                        raise
                limit = await client.read_global_concurrency_limit_by_name(self.name)

            # Never let one deployment silently change another deployment's quota.
            if (
                not limit.active
                or limit.limit != 1
                or not math.isclose(
                    limit.slot_decay_per_second, self.requests_per_second, rel_tol=1e-6
                )
            ):
                raise ValueError(f"Prefect rate limit {self.name!r} has incompatible settings")


class RateLimitAcquisitionError(RuntimeError):
    """Prefect could not confirm an allowance; no provider request was sent.

    This is deliberately not an HTTPX network error. HTTP retry libraries must
    not mistake a Prefect outage for a failed provider request and multiply the
    limiter's bounded retries. The original exception remains available as cause.
    """


class RateLimitedAsyncTransport(AsyncBaseTransport):
    """Acquire an allowance immediately before the wrapped transport sends.

    Put retry transports outside this transport, and a non-retrying network
    transport inside it. Each retry will then come back here for a new allowance.
    Closing this transport also closes its wrapped transport.
    """

    def __init__(self, limiter: ApiRateLimit, transport: AsyncBaseTransport) -> None:
        """Accept the limiter and network transport without selecting a retry library."""
        self.limiter = limiter
        self.transport = transport

    async def handle_async_request(self, request: Request) -> Response:
        """Obtain a Prefect allowance, then return the downstream HTTP response.

        Raise RateLimitAcquisitionError without sending if Prefect acquisition
        fails. Pass downstream HTTP errors through unchanged so the caller's
        retry system can handle them. Cancellation stops the operation.
        """
        try:
            await self.limiter.acquire()
        except Exception as error:
            # Keep Prefect failures separate from the HTTP errors returned by the
            # provider transport below. Cancellation is not caught here.
            raise RateLimitAcquisitionError(
                "Could not acquire a Prefect rate-limit allowance"
            ) from error

        return await self.transport.handle_async_request(request)

    async def aclose(self) -> None:
        """Release the network resources when the HTTP client closes."""
        await self.transport.aclose()
