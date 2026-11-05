import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest
from prefect.exceptions import ObjectNotFound
from pydantic import ValidationError

import mad_prefect.rate_limit as module
from mad_prefect.rate_limit import ApiRateLimit


@pytest.fixture
def backend(monkeypatch):
    client = AsyncMock()
    client.__aenter__.return_value = client
    client.read_global_concurrency_limit_by_name.return_value = SimpleNamespace(
        active=True, limit=1, slot_decay_per_second=2 / 3
    )
    acquire = AsyncMock()
    monkeypatch.setattr(module, "get_client", lambda: client)
    monkeypatch.setattr(module, "rate_limit", acquire)
    monkeypatch.setattr(module, "PREFECT_API_URL", SimpleNamespace(value=lambda: "http://localhost/api"))
    return client, acquire


def limiter(**kwargs):
    return ApiRateLimit(name="test-account", requests_per_second=2 / 3, **kwargs)


async def test_existing_limit_is_checked_before_each_acquisition(backend):
    client, acquire = backend
    rate = limiter()
    await rate.acquire()
    await rate.acquire()
    assert client.read_global_concurrency_limit_by_name.await_count == 2
    client.create_global_concurrency_limit.assert_not_called()
    assert acquire.await_count == 2
    acquire.assert_awaited_with("test-account", occupy=1, strict=True, timeout_seconds=120)


@pytest.mark.parametrize("race", [False, True])
async def test_creates_missing_limit_and_handles_another_pod_creating_it(backend, race):
    client, acquire = backend
    settings = client.read_global_concurrency_limit_by_name.return_value
    client.read_global_concurrency_limit_by_name.side_effect = [ObjectNotFound(Exception("missing")), settings]
    if race:
        response = httpx.Response(409, request=httpx.Request("POST", "http://local/limits"))
        client.create_global_concurrency_limit.side_effect = httpx.HTTPStatusError(
            "exists", request=response.request, response=response
        )
    await limiter().acquire()
    created = client.create_global_concurrency_limit.call_args.args[0]
    assert (created.name, created.limit, created.active, created.slot_decay_per_second) == (
        "test-account", 1, True, 2 / 3
    )
    acquire.assert_awaited_once()


@pytest.mark.parametrize("field,value", [("active", False), ("limit", 2), ("slot_decay_per_second", 2)])
async def test_mismatched_configuration_fails_without_acquiring_or_overwriting(backend, field, value):
    client, acquire = backend
    setattr(client.read_global_concurrency_limit_by_name.return_value, field, value)
    with pytest.raises(ValueError, match="incompatible"):
        await limiter().acquire()
    acquire.assert_not_called()
    client.update_global_concurrency_limit.assert_not_called()


async def test_backend_failure_prevents_acquisition(backend):
    client, acquire = backend
    client.read_global_concurrency_limit_by_name.side_effect = OSError("unavailable")
    with pytest.raises(OSError):
        await limiter().acquire()
    acquire.assert_not_called()


async def test_creation_failure_is_not_mistaken_for_a_race(backend):
    client, acquire = backend
    client.read_global_concurrency_limit_by_name.side_effect = ObjectNotFound(Exception("missing"))
    response = httpx.Response(403, request=httpx.Request("POST", "http://local/limits"))
    client.create_global_concurrency_limit.side_effect = httpx.HTTPStatusError(
        "forbidden", request=response.request, response=response
    )
    with pytest.raises(httpx.HTTPStatusError):
        await limiter().acquire()
    acquire.assert_not_called()


@pytest.mark.parametrize("phase", ["setup", "acquisition"])
async def test_timeout_bounds_all_server_work(backend, phase):
    client, acquire = backend
    async def blocked(*args, **kwargs):
        await asyncio.Event().wait()
    if phase == "setup":
        client.read_global_concurrency_limit_by_name.side_effect = blocked
    else:
        acquire.side_effect = blocked
    with pytest.raises(TimeoutError):
        await limiter(wait_timeout_seconds=0.01).acquire()


async def test_requires_shared_server(backend, monkeypatch):
    client, acquire = backend
    monkeypatch.setattr(module, "PREFECT_API_URL", SimpleNamespace(value=lambda: None))
    with pytest.raises(RuntimeError, match="PREFECT_API_URL"):
        await limiter().acquire()
    client.read_global_concurrency_limit_by_name.assert_not_called()
    acquire.assert_not_called()


@pytest.mark.parametrize("rate", [0, -1, float("inf"), float("nan")])
def test_invalid_rates_rejected(rate):
    with pytest.raises(ValidationError):
        ApiRateLimit(name="account", requests_per_second=rate)


@pytest.mark.parametrize("name", ["", " ", "account/key"])
def test_invalid_names_rejected(name):
    with pytest.raises(ValidationError):
        ApiRateLimit(name=name, requests_per_second=1)
