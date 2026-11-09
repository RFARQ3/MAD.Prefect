import asyncio
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
from unittest.mock import AsyncMock

import httpx
import pytest
from httpx_retries import RetryTransport

import mad_prefect.http as module
from mad_prefect.http import HttpRetryPolicy, RetryingAsyncClient


def policy(**kwargs):
    return HttpRetryPolicy(backoff_jitter=0, **kwargs)


@pytest.mark.parametrize('failure', [429, 500, 502, 503, 504, 'network', 'body'])
async def test_every_attempt_is_paced_including_body_errors(monkeypatch, failure):
    events = []
    limiter = AsyncMock()
    async def acquire():
        events.append('acquire')
    limiter.acquire.side_effect = acquire
    class BrokenBody(httpx.AsyncByteStream):
        async def __aiter__(self):
            raise httpx.ReadTimeout('synthetic')
            yield b''
    def handle(request):
        events.append('GET')
        assert request.url.params['page'] == '2'
        if events.count('GET') == 1:
            if failure == 'network':
                raise httpx.ConnectError('synthetic')
            if failure == 'body':
                return httpx.Response(200, stream=BrokenBody())
            return httpx.Response(failure, headers={'Retry-After': '0'})
        return httpx.Response(200, json=[])
    monkeypatch.setattr(module.asyncio, 'sleep', AsyncMock())
    async with RetryingAsyncClient(base_url='https://synthetic.invalid', rate_limit=limiter,
                                   retry=policy(), transport=httpx.MockTransport(handle)) as client:
        response = await client.get('users', params={'page': 2})
    assert response.status_code == 200
    assert events == ['acquire', 'GET', 'acquire', 'GET']


@pytest.mark.parametrize('status', [401, 403, 404])
async def test_permanent_response_is_returned_without_retry(status):
    limiter = AsyncMock()
    async with RetryingAsyncClient(rate_limit=limiter, transport=httpx.MockTransport(
        lambda request: httpx.Response(status)
    )) as client:
        response = await client.get('https://synthetic.invalid')
    assert response.status_code == status
    limiter.acquire.assert_awaited_once()


async def test_retry_exhaustion_and_capped_backoff(monkeypatch):
    sleep = AsyncMock()
    monkeypatch.setattr(module.asyncio, 'sleep', sleep)
    limiter = AsyncMock()
    async with RetryingAsyncClient(rate_limit=limiter, retry=policy(max_backoff_seconds=1.5),
                                  transport=httpx.MockTransport(lambda request: httpx.Response(503))) as client:
        response = await client.get('https://synthetic.invalid')
    assert response.status_code == 503
    assert limiter.acquire.await_count == 3
    assert [call.args[0] for call in sleep.call_args_list] == [1, 1.5]


@pytest.mark.parametrize('header', [None, 'invalid', 'nan', 'inf', '-1'])
async def test_provider_fallback_for_unusable_retry_after(monkeypatch, header):
    sleep = AsyncMock()
    monkeypatch.setattr(module.asyncio, 'sleep', sleep)
    statuses = iter([429, 200])
    async with RetryingAsyncClient(retry=policy(rate_limit_fallback_seconds=300, total_timeout_seconds=720),
                                  transport=httpx.MockTransport(lambda request: httpx.Response(
                                      next(statuses), headers={'Retry-After': header} if header else {}))) as client:
        await client.get('https://synthetic.invalid')
    sleep.assert_awaited_once_with(300)


@pytest.mark.parametrize('header', ['0', '300', 'date'])
async def test_retry_after_is_not_clipped_to_backoff_cap(monkeypatch, header):
    sleep = AsyncMock()
    monkeypatch.setattr(module.asyncio, 'sleep', sleep)
    if header == 'date':
        value = format_datetime(datetime.now(timezone.utc) + timedelta(seconds=300))
    else:
        value = header
    statuses = iter([429, 200])
    async with RetryingAsyncClient(retry=policy(total_timeout_seconds=720), transport=httpx.MockTransport(
        lambda request: httpx.Response(next(statuses), headers={'Retry-After': value})
    )) as client:
        await client.get('https://synthetic.invalid')
    delay = sleep.call_args.args[0]
    assert (298 <= delay <= 300) if header == 'date' else delay == float(header)


async def test_delay_beyond_deadline_fails_without_retry():
    limiter = AsyncMock()
    async with RetryingAsyncClient(rate_limit=limiter, transport=httpx.MockTransport(
        lambda request: httpx.Response(429, headers={'Retry-After': '900'})
    )) as client:
        with pytest.raises(TimeoutError, match='remaining'):
            await client.get('https://synthetic.invalid')
    limiter.acquire.assert_awaited_once()


async def test_limiter_failure_is_not_retried_by_http_engine():
    limiter = AsyncMock()
    error = httpx.ConnectError('synthetic')
    limiter.acquire.side_effect = error
    send = AsyncMock()
    async with RetryingAsyncClient(rate_limit=limiter, transport=httpx.MockTransport(send)) as client:
        with pytest.raises(httpx.ConnectError) as raised:
            await client.get('https://synthetic.invalid')
    assert raised.value is error
    limiter.acquire.assert_awaited_once()
    send.assert_not_called()


async def test_total_deadline_includes_limiter():
    limiter = AsyncMock()
    async def wait():
        await asyncio.Event().wait()
    limiter.acquire.side_effect = wait
    async with RetryingAsyncClient(rate_limit=limiter, retry=policy(total_timeout_seconds=0.01)) as client:
        with pytest.raises(TimeoutError):
            await client.get('https://synthetic.invalid')


async def test_cancellation_propagates():
    limiter = AsyncMock()
    limiter.acquire.side_effect = asyncio.CancelledError()
    async with RetryingAsyncClient(rate_limit=limiter) as client:
        with pytest.raises(asyncio.CancelledError):
            await client.get('https://synthetic.invalid')
    limiter.acquire.assert_awaited_once()


async def test_redirect_is_also_paced():
    limiter = AsyncMock()
    def handle(request):
        return httpx.Response(302, headers={'Location': '/next'}) if request.url.path == '/' else httpx.Response(200)
    async with RetryingAsyncClient(rate_limit=limiter, follow_redirects=True,
                                  transport=httpx.MockTransport(handle)) as client:
        assert (await client.get('https://synthetic.invalid')).status_code == 200
    assert limiter.acquire.await_count == 2


async def test_empty_statuses_disable_response_retries():
    limiter = AsyncMock()
    async with RetryingAsyncClient(rate_limit=limiter, retry=policy(retry_statuses=()),
                                  transport=httpx.MockTransport(lambda request: httpx.Response(503))) as client:
        assert (await client.get('https://synthetic.invalid')).status_code == 503
    limiter.acquire.assert_awaited_once()


async def test_rejects_write_methods_and_double_retry_transport():
    async with RetryingAsyncClient() as client:
        with pytest.raises(ValueError, match='GET and HEAD'):
            await client.post('https://synthetic.invalid', json={})
    async with RetryingAsyncClient(transport=RetryTransport()) as client:
        with pytest.raises(ValueError, match='does not also retry'):
            await client.get('https://synthetic.invalid')


async def test_concurrent_calls_have_independent_retry_budgets(monkeypatch):
    monkeypatch.setattr(module.asyncio, 'sleep', AsyncMock())
    counts = {}
    limiter = AsyncMock()
    def handle(request):
        path = request.url.path
        counts[path] = counts.get(path, 0) + 1
        return httpx.Response(503 if counts[path] == 1 else 200)
    async with RetryingAsyncClient(rate_limit=limiter, retry=policy(max_attempts=2),
                                  transport=httpx.MockTransport(handle)) as client:
        responses = await asyncio.gather(client.get('https://synthetic.invalid/a'),
                                         client.get('https://synthetic.invalid/b'))
    assert [response.status_code for response in responses] == [200, 200]
    assert counts == {'/a': 2, '/b': 2}
    assert limiter.acquire.await_count == 4


@pytest.mark.parametrize('stage', ['http', 'backoff'])
async def test_total_deadline_covers_http_and_backoff(stage):
    async def handle(request):
        if stage == 'http':
            await asyncio.Event().wait()
        return httpx.Response(503)
    async with RetryingAsyncClient(retry=policy(total_timeout_seconds=0.01),
                                  transport=httpx.MockTransport(handle)) as client:
        with pytest.raises(TimeoutError):
            await client.get('https://synthetic.invalid')


async def test_retry_logs_omit_request_details(monkeypatch, caplog):
    import logging
    monkeypatch.setattr(module.asyncio, 'sleep', AsyncMock())
    statuses = iter([503, 200])
    with caplog.at_level(logging.INFO, logger=module.__name__):
        async with RetryingAsyncClient(transport=httpx.MockTransport(
            lambda request: httpx.Response(next(statuses))
        )) as client:
            await client.get('https://synthetic.invalid/private?token=secret')
    messages = [record.getMessage() for record in caplog.records if record.name == module.__name__]
    assert any('retry 1/2' in message for message in messages)
    assert any('recovered' in message for message in messages)
    assert all('secret' not in message and 'private' not in message for message in messages)


async def test_empty_status_policy_survives_network_retry(monkeypatch):
    monkeypatch.setattr(module.asyncio, 'sleep', AsyncMock())
    calls = 0
    def handle(request):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise httpx.ConnectError('synthetic')
        return httpx.Response(503)
    async with RetryingAsyncClient(retry=policy(retry_statuses=()),
                                  transport=httpx.MockTransport(handle)) as client:
        assert (await client.get('https://synthetic.invalid')).status_code == 503
    assert calls == 2
