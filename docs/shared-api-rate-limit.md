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
Creation races are safe. Every acquisition checks the existing configuration.
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

Shared limiter implemented; validation results recorded with the feature handoff.

## Next Steps

Review the KnowBe4 consumer demonstration before merging and releasing.

## Blockers & Risks

Production validation requires separately authorized access. No live customer
systems are needed for unit tests or the local synthetic demonstration.
