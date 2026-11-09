"""Shared request pacing through Prefect's global concurrency limits."""

import asyncio
import logging
import math
import random

from httpx import HTTPStatusError, NetworkError, RemoteProtocolError, TimeoutException
from prefect.client.orchestration import get_client
from prefect.client.schemas.actions import GlobalConcurrencyLimitCreate
from prefect.concurrency.asyncio import rate_limit
from prefect.concurrency._asyncio import ConcurrencySlotAcquisitionError
from prefect.exceptions import MissingContextError, ObjectNotFound
from prefect.logging import get_run_logger
from prefect.settings import PREFECT_API_URL
from pydantic import BaseModel, ConfigDict, Field, PrivateAttr


def _transient_error(error: Exception) -> Exception | None:
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
    """Use one name per provider account on the same Prefect server."""

    model_config = ConfigDict(frozen=True)

    name: str = Field(min_length=1, pattern=r"^[a-zA-Z0-9][a-zA-Z0-9_-]*$")
    requests_per_second: float = Field(gt=0, allow_inf_nan=False)
    wait_timeout_seconds: float = Field(default=120, gt=0, allow_inf_nan=False)

    max_attempts: int = Field(default=4, ge=1, le=100)
    retry_delay_seconds: float = Field(default=1, gt=0, allow_inf_nan=False)
    max_retry_delay_seconds: float = Field(default=10, gt=0, allow_inf_nan=False)

    _ready: bool = PrivateAttr(default=False)
    _setup_lock: asyncio.Lock = PrivateAttr(default_factory=asyncio.Lock)

    async def acquire(self) -> None:
        """Retry temporary Prefect failures without creating per-request tasks."""
        if not PREFECT_API_URL.value():
            raise RuntimeError("Shared API rate limits require a configured PREFECT_API_URL")
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

        await rate_limit(
            self.name, occupy=1, strict=True,
            timeout_seconds=self.wait_timeout_seconds,
        )

    async def _validate_limit(self) -> None:
        async with get_client() as client:
            try:
                limit = await client.read_global_concurrency_limit_by_name(self.name)
            except ObjectNotFound:
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
