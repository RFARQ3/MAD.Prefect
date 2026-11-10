import asyncio
import logging
from unittest.mock import AsyncMock, Mock

import httpx
import pytest

import mad_prefect.rate_limit as module
from mad_prefect.rate_limit import ApiRateLimit
from prefect.concurrency._asyncio import ConcurrencySlotAcquisitionError
from test_rate_limit import backend


def wrapped(error):
    wrapper = ConcurrencySlotAcquisitionError('sensitive wrapper text')
    wrapper.__cause__ = error
    return wrapper


@pytest.mark.parametrize('phase', ['setup', 'acquisition'])
async def test_connection_recovers_and_logs_without_exception_payload(backend, monkeypatch, caplog, phase):
    client, acquire = backend
    sleep = AsyncMock()
    monkeypatch.setattr(module.asyncio, 'sleep', sleep)
    error = httpx.ConnectError('secret URL and credentials')
    if phase == 'setup':
        client.read_global_concurrency_limit_by_name.side_effect = [error, client.read_global_concurrency_limit_by_name.return_value]
    else:
        acquire.side_effect = [wrapped(error), None]
    with caplog.at_level(logging.INFO):
        rate = ApiRateLimit(name='test-account', requests_per_second=2 / 3)
        await rate.acquire()
    assert sleep.await_count == 1
    assert 0.5 <= sleep.call_args.args[0] <= 1
    assert 'attempt 1/4' in caplog.text
    assert 'ConnectError' in caplog.text
    assert 'recovered on attempt 2/4' in caplog.text
    assert 'secret URL' not in caplog.text
    assert 'sensitive wrapper' not in caplog.text


async def test_exhaustion_is_bounded_and_backoff_is_capped(backend, monkeypatch, caplog):
    _client, acquire = backend
    error = wrapped(httpx.ConnectError('private'))
    acquire.side_effect = error
    sleep = AsyncMock()
    monkeypatch.setattr(module.asyncio, 'sleep', sleep)
    monkeypatch.setattr(module.random, 'uniform', lambda low, high: high)
    with pytest.raises(ConcurrencySlotAcquisitionError) as raised:
        await ApiRateLimit(name='test-account', requests_per_second=2 / 3,
                           max_attempts=5, max_retry_delay_seconds=3).acquire()
    assert raised.value is error
    assert acquire.await_count == 5
    assert [call.args[0] for call in sleep.call_args_list] == [1, 2, 3, 3]
    assert 'attempt 5/5' in caplog.text
    assert 'no request permitted' in caplog.text


@pytest.mark.parametrize('status', [401, 403, 404, 422, 429, 500, 502, 503, 504])
async def test_only_selected_server_errors_retry(backend, monkeypatch, status):
    _client, acquire = backend
    response = httpx.Response(status, request=httpx.Request('GET', 'https://synthetic.invalid'))
    error = httpx.HTTPStatusError('sensitive', request=response.request, response=response)
    acquire.side_effect = [wrapped(error), None]
    monkeypatch.setattr(module.asyncio, 'sleep', AsyncMock())
    rate = ApiRateLimit(name='test-account', requests_per_second=2 / 3)
    if status >= 500:
        await rate.acquire()
        assert acquire.await_count == 2
    else:
        with pytest.raises(ConcurrencySlotAcquisitionError):
            await rate.acquire()
        assert acquire.await_count == 1


async def test_missing_limit_wrapper_is_not_retried(backend):
    _client, acquire = backend
    acquire.side_effect = ConcurrencySlotAcquisitionError('missing limit')
    with pytest.raises(ConcurrencySlotAcquisitionError):
        await ApiRateLimit(name='test-account', requests_per_second=2 / 3).acquire()
    assert acquire.await_count == 1


async def test_deadline_includes_backoff(backend, caplog):
    _client, acquire = backend
    acquire.side_effect = wrapped(httpx.ConnectError('unavailable'))
    with pytest.raises(TimeoutError):
        await ApiRateLimit(name='test-account', requests_per_second=2 / 3,
                           wait_timeout_seconds=0.01).acquire()
    assert acquire.await_count == 1
    assert 'timed out' in caplog.text


async def test_cancellation_is_not_retried(backend):
    _client, acquire = backend
    acquire.side_effect = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await ApiRateLimit(name='test-account', requests_per_second=2 / 3).acquire()
    assert acquire.await_count == 1


async def test_logs_use_prefect_run_logger_when_available(backend, monkeypatch):
    _client, acquire = backend
    acquire.side_effect = [wrapped(httpx.ReadTimeout('private')), None]
    logger = Mock()
    monkeypatch.setattr(module, 'get_run_logger', lambda: logger)
    monkeypatch.setattr(module.asyncio, 'sleep', AsyncMock())
    await ApiRateLimit(name='test-account', requests_per_second=2 / 3).acquire()
    logger.warning.assert_called_once()
    logger.info.assert_called_once()
    logger.error.assert_not_called()


@pytest.mark.parametrize('error, expected', [
    (httpx.ConnectError('synthetic'), True),
    (httpx.ReadTimeout('synthetic'), True),
    (httpx.RemoteProtocolError('synthetic'), True),
    (ValueError('invalid settings'), False),
    (ConcurrencySlotAcquisitionError('missing limit'), False),
    (wrapped(httpx.ConnectError('synthetic')), True),
    (wrapped(wrapped(httpx.ReadTimeout('synthetic'))), True),
    (wrapped(ValueError('invalid settings')), False),
])
def test_prefect_retry_check_returns_a_boolean(error, expected):
    """Classification answers yes or no without returning or raising an error."""
    assert module.is_retryable_prefect_error(error) is expected


def test_self_referencing_prefect_error_is_not_retryable():
    """A malformed cause must not leave classification looping forever."""
    error = ConcurrencySlotAcquisitionError('synthetic')
    error.__cause__ = error
    assert module.is_retryable_prefect_error(error) is False
