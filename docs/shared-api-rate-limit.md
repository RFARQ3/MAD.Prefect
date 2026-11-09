# Shared API rate limits

```python
from mad_prefect.rate_limit import ApiRateLimit

limit = ApiRateLimit(name="example-account", requests_per_second=2 / 3)
await limit.acquire()
response = await http_client.get("users")
```

Call `acquire()` immediately before **every physical HTTP attempt**, including
retries. HTTPX still sends the request. Prefect grants the shared allowance.
Use the same non-secret name for every deployment using one provider account.
All participating Kubernetes pods must use the same persistent Prefect server.
Different Prefect servers do not share an allowance, even with identical names.

The first caller creates an active global concurrency limit with capacity one.
Its slot decays at `requests_per_second`; capacity one avoids an initial burst.
Creation races are safe. The first acquisition validates configuration; later calls reuse that validation.
Reuse the same limiter instance across requests. Concurrent callers share one setup
lock, and unsuccessful setup is never cached.
An inactive or differently configured limit fails instead of being overwritten.
Missing server configuration, server errors and timeouts prevent the HTTP attempt.
`wait_timeout_seconds` bounds configuration checks and acquisition together.

The feature requires Prefect 3.2.13 or later (within this package's supported
Prefect range). Existing MAD.Prefect functionality remains available with older
Prefect versions; do not import this module there.

Administrators must not delete, disable or edit the limit while consumers run:
configuration checking and acquisition are separate server operations. Coordinate
rate changes across deployments while stopped. Pacing is not a daily quota counter,
a concurrency cap on in-flight HTTP requests, or a shared provider cooldown.
The caller owns HTTP retries and Retry-After handling. Other integrations that
do not use this limit still consume the provider's allowance.

## Progress

Shared limiter and bounded recovery implemented. Synthetic tests cover wrapped connection failures, recovery logging, capped backoff, exhaustion, cancellation, configuration errors and the total deadline.

## Next Steps

Review the KnowBe4 consumer demonstration before merging and releasing.

## Blockers & Risks

Production validation requires separately authorized access. No live customer
systems are needed for unit tests or the local synthetic demonstration.

## Recovery from temporary Prefect failures

`acquire()` retries network failures, HTTP transport timeouts, remote protocol
errors and HTTP 500/502/503/504 responses, including transport errors explicitly
wrapped by Prefect's `ConcurrencySlotAcquisitionError`. Missing limits without a
transport cause, invalid settings, authentication failures and cancellation are
not retried. Setup failures retry validation; after successful validation, acquisition retries
only request an allowance and do not reread configuration.

Defaults are four total attempts, a one-second initial backoff and a ten-second
backoff cap. Backoff doubles and is jittered between half and all of that delay.
Configure `max_attempts`, `retry_delay_seconds` and `max_retry_delay_seconds` on
`ApiRateLimit`. `wait_timeout_seconds` remains one total deadline including all
attempts, configuration checks, sleeps and time waiting for an allowance.

Warnings record the limit name, attempt, error class and next delay; an INFO
message records recovery and an ERROR records exhaustion or timeout. In a Prefect
run these use its run logger; outside runs they use the module's Python logger.
Exception text, request URLs, response bodies and credentials are not logged by
this retry mechanism. No additional Prefect tasks are created.

A lost response can mean the server granted an allowance that the caller never
received. Retrying can consume another allowance, reducing throughput, but never
permits an unconfirmed downstream request. Persistent server outages still fail
within the configured budget. This does not retry or change provider HTTP calls.

## Setup cache lifetime

Validation is cached per instance, not per process or account. After setup, the
normal path makes one Prefect slot-acquisition request per physical HTTP attempt;
Prefect may make additional requests while waiting for capacity or recovering.
Create a fresh instance after configuration or server changes. Changes to an
active limit are not polled: stop consumers before editing or disabling limits,
then restart them. A deleted limit still fails strict acquisition; it is not
silently recreated by a previously initialized instance.
