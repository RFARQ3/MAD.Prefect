"""Shared request pacing through Prefect's global concurrency limits."""

import asyncio
import math

from httpx import HTTPStatusError
from prefect.client.orchestration import get_client
from prefect.client.schemas.actions import GlobalConcurrencyLimitCreate
from prefect.concurrency.asyncio import rate_limit
from prefect.exceptions import ObjectNotFound
from prefect.settings import PREFECT_API_URL
from pydantic import BaseModel, ConfigDict, Field


class ApiRateLimit(BaseModel):
    """Use one name per provider account on the same Prefect server."""

    model_config = ConfigDict(frozen=True)

    name: str = Field(min_length=1, pattern=r"^[a-zA-Z0-9][a-zA-Z0-9_-]*$")
    requests_per_second: float = Field(gt=0, allow_inf_nan=False)
    wait_timeout_seconds: float = Field(default=120, gt=0, allow_inf_nan=False)

    async def acquire(self) -> None:
        """Wait for permission before one HTTP attempt; fail if coordination fails."""
        if not PREFECT_API_URL.value():
            raise RuntimeError("Shared API rate limits require a configured PREFECT_API_URL")

        # Bound both server configuration checks and waiting for an allowance.
        async with asyncio.timeout(self.wait_timeout_seconds):
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

            await rate_limit(
                self.name, occupy=1, strict=True,
                timeout_seconds=self.wait_timeout_seconds,
            )
