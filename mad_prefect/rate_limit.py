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


def _transient_error(error: Exception) -> Exception | None:
    """Find a temporary Prefect failure without treating configuration errors as retryable."""
    # Prefect wraps transport errors during slot acquisition. Missing limits use
    # the same wrapper but must fail immediately, so inspect only explicit causes.
    while isinstance(error, ConcurrencySlotAcquisitionError):
        cause = error.__cause__
        if not isinstance(cause, Exception) or cause is error:
            return None
        error = cause
    if isinstance(error, (NetworkError, TimeoutException, RemoteProtocolError)):
        return error
    if isinstance(error, HTTPStatusError) and error.response.status_code in (500, 502, 503, 504):
        return error
    return None


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

    _ready: bool = PrivateAttr(default=False)
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

        delay = min(self.retry_delay_seconds, self.max_retry_delay_seconds)
        attempt = 0
        try:
            # One deadline covers setup, all attempts, backoff and slot waiting.
            async with asyncio.timeout(self.wait_timeout_seconds):
                for attempt in range(1, self.max_attempts + 1):
                    try:
                        await self._acquire_once()
                    except Exception as error:
                        transient = _transient_error(error)
                        if transient is None or attempt == self.max_attempts:
                            logger.error(
                                "Rate limit %s failed on attempt %d/%d (%s); no request permitted",
                                self.name, attempt, self.max_attempts,
                                type(transient if transient is not None else error).__name__,
                            )
                            raise

                        # Jitter keeps recovering pods from reconnecting together.
                        wait = random.uniform(delay / 2, delay)
                        logger.warning(
                            "Rate limit %s attempt %d/%d failed (%s); retrying in %.2fs",
                            self.name, attempt, self.max_attempts, type(transient).__name__, wait,
                        )
                        await asyncio.sleep(wait)
                        delay = min(delay * 2, self.max_retry_delay_seconds)
                    else:
                        if attempt > 1:
                            logger.info("Rate limit %s recovered on attempt %d/%d",
                                        self.name, attempt, self.max_attempts)
                        return
        except TimeoutError:
            logger.error("Rate limit %s timed out on attempt %d/%d within %.2fs; no request permitted",
                         self.name, attempt, self.max_attempts, self.wait_timeout_seconds)
            raise

    async def _acquire_once(self) -> None:
        """Validate once, then acquire an allowance for every physical request."""
        if not self._ready:
            # Concurrent callers share setup; failures leave it eligible for retry.
            async with self._setup_lock:
                if not self._ready:
                    await self._validate_limit()
                    self._ready = True

        # A successful allowance is consumed, not released. Prefect restores it
        # at the configured rate, which spaces requests across all participating pods.
        await rate_limit(
            self.name, occupy=1, strict=True,
            timeout_seconds=self.wait_timeout_seconds,
        )

    async def _validate_limit(self) -> None:
        """Create a missing limit, but never overwrite another deployment's settings."""
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
        """Confirm permission first, then send exactly one HTTP attempt."""
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
