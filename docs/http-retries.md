# Shared HTTP retries

```python
from mad_prefect.http import HttpRetryPolicy, RetryingAsyncClient
from mad_prefect.rate_limit import ApiRateLimit

limit = ApiRateLimit(name="provider-account", requests_per_second=2 / 3)
policy = HttpRetryPolicy(max_attempts=3, total_timeout_seconds=720,
                         rate_limit_fallback_seconds=300)
async with RetryingAsyncClient(base_url="https://api.example.com/",
                               retry=policy, rate_limit=limit) as client:
    response = await client.get("users", params={"page": 1})
    response.raise_for_status()
```

`httpx-retries` supplies the retry loop, response cleanup, transient HTTP exception
selection, backoff calculation and Retry-After parsing. The shared client connects
that loop to an optional limiter via a request hook, so each attempt and redirect
must acquire an allowance. Reuse a limiter across clients to retain setup caching.
Omit `rate_limit` for HTTP retries without Prefect rate-limit access.

Use `get`, `head` or `request` for buffered reads. Other methods are rejected;
streaming and direct `send` are not covered by the retry/deadline contract. Use a
plain HTTPX client for those operations. Do not combine this client with another
retrying transport or authentication scheme that resends requests. Ordinary
HTTPX options, including headers, timeouts and mock transports, remain available.

The policy controls attempts, statuses, exponential backoff, jitter, total elapsed
time and an optional 429 fallback. Retry-After dates and integer seconds are honored
without clipping to the backoff cap. If a delay cannot fit the remaining deadline,
the call fails without retrying early. One deadline includes limiter acquisition,
HTTP requests and all sleeps. Final HTTP responses are returned as HTTPX normally
does; callers choose whether to call `raise_for_status()`.

Limiter failures retain the limiter's own bounded recovery and are not retried by
the HTTP loop. Cancellation propagates. Retry warnings and recovery/failure logs
use the current Prefect run logger, or a Python logger outside runs, without
including URLs, credentials or response bodies. Normal HTTPX/library logging is
configured separately. No per-request Prefect tasks are created.

## Progress

Shared HTTP client implemented with provider settings in KnowBe4. Validation uses
synthetic HTTP responses and a disposable local Prefect server.

## Next Steps

Review feature branches before release and authorized production rollout.

## Blockers & Risks

This is a buffered GET/HEAD client, not a generic business-write retry mechanism.
429 cooldowns apply to the caller; they are not broadcast across processes.
The request hook is part of the pacing contract and must not be removed.
