"""Shared request pacing through Prefect's global concurrency limits."""

import asyncio
import math

from httpx import (
    AsyncBaseTransport,
    HTTPStatusError,
    Request,
    Response,
)
from prefect.client.orchestration import get_client
from prefect.client.schemas.actions import GlobalConcurrencyLimitCreate
from prefect.concurrency.asyncio import rate_limit
from prefect.exceptions import ObjectNotFound
from prefect.settings import PREFECT_API_URL

from mad_prefect.prefect_retries import retry_prefect_acquisition


class ApiRateLimit:
    """Share request spacing between processes using the same Prefect server.

    Use one non-secret name per provider account and reuse this instance across
    requests. Only successful setup is cached; each acquire still asks Prefect
    for a new allowance. HTTP retries and provider cooldowns belong to the caller.
    """

    name: str
    requests_per_minute: int
    # Total attempts to obtain an allowance from Prefect after temporary failures.
    # Includes the first attempt; does not retry downstream API requests.
    max_attempts: int

    _configuration_checked: bool
    _setup_lock: asyncio.Lock

    def __init__(self, name: str, requests_per_minute: int, *, max_attempts: int = 4) -> None:
        """Set the account's shared rate and the maximum Prefect connection attempts.

        For example, requests_per_minute=40 spaces requests about 1.5 seconds
        apart across all callers using this name on the same Prefect server.
        max_attempts includes the first attempt and affects only Prefect calls.
        """
        if max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")

        self.name = name
        self.requests_per_minute = requests_per_minute
        self.max_attempts = max_attempts

        # Reuse successful setup. The lock stops concurrent first requests from
        # checking or creating the same limit more than once in this instance.
        self._configuration_checked = False
        self._setup_lock = asyncio.Lock()

    @retry_prefect_acquisition
    async def acquire(self) -> None:
        """Check setup once, then ask Prefect for one rate-limit allowance.

        Return only when Prefect grants permission. The decorator handles
        temporary failures, retry delays and logging using max_attempts.
        No Prefect task is created and no allowance needs to be released.
        """
        if not PREFECT_API_URL.value():
            raise RuntimeError("Shared API rate limits require a configured PREFECT_API_URL")

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
            timeout_seconds=_PREFECT_ALLOWANCE_WAIT_SECONDS,
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
                            slot_decay_per_second=self.requests_per_minute / 60,
                        )
                    )
                except HTTPStatusError as error:
                    # Another pod can create the account's limit at the same time.
                    if error.response.status_code != 409:
                        raise
                limit = await client.read_global_concurrency_limit_by_name(self.name)

            # The same account name must mean the same rate in every deployment.
            # Fail on a mismatch instead of overwriting another caller's settings.
            if (
                not limit.active
                or limit.limit != 1
                or not math.isclose(
                    limit.slot_decay_per_second, self.requests_per_minute / 60, rel_tol=1e-6
                )
            ):
                raise ValueError(f"Prefect rate limit {self.name!r} has incompatible settings")


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


# Prefect's HTTP timeout handles unreachable servers. Its allowance service can
# otherwise repeat HTTP 423 responses forever while a limit is busy. Bound that
# wait internally so downstream developers only need to specify their API quota.
_PREFECT_ALLOWANCE_WAIT_SECONDS = 120


class RateLimitAcquisitionError(RuntimeError):
    """Prefect could not confirm an allowance; no provider request was sent.

    This is deliberately not an HTTPX network error. HTTP retry libraries must
    not mistake a Prefect outage for a failed provider request and multiply the
    limiter's bounded retries. The original exception remains available as cause.
    """
