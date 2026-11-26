# Environment-backed Prefect blocks

## Developer API

Subclass `EnvBlock`, declare credential fields as Pydantic model fields, and use an
optional `prefix` model field when the environment-variable prefix should differ
from the class name.

```python
from mad_prefect.envblock import EnvBlock


class XeroCredentials(EnvBlock):
    prefix: str = "XERO"
    client_id: str
    client_secret: str
    scopes: str


credentials = await XeroCredentials.from_env()
```

This example reads `XERO_CLIENT_ID`, `XERO_CLIENT_SECRET`, and `XERO_SCOPES`.
Without an explicit prefix, `EnvBlock` uses the uppercased subclass name. For
example, `XeroCredentials` reads `XEROCREDENTIALS_CLIENT_ID`.

If `<PREFIX>_CREDENTIAL_BLOCK_NAME` is set, `from_env()` loads that saved Prefect
block instead of constructing one from individual environment variables. Required
fields remain required in either declaration style.

An ordinary string field is sufficient for an explicit prefix:

```python
class ServiceCredentials(EnvBlock):
    prefix: str = "SERVICE"
    token: str
```

Using a `ClassVar` removes `prefix` from Pydantic's model fields and breaks prefix
resolution. Keep it as a model field.

## Value conversion

`from_env()` passes supplied strings to the existing Pydantic model for validation
and conversion. Downstream field declarations do not need to change.

```python
class MetadataConfig(EnvBlock):
    prefix: str = "PREFECT_METADATA"
    trailing_days: int = 180
    backfill: bool = True


config = await MetadataConfig.from_env({"PREFECT_METADATA_BACKFILL": "false"})
assert config.backfill is False
```

Pydantic also handles `SecretStr`, optional fields, dates and durations. Unset
values retain their defaults, and a missing required environment variable still
raises `ValueError`. Invalid supplied values raise Pydantic's `ValidationError`,
which is a `ValueError` subclass. Saved-block loading and instance caching are
unchanged.

Union fields follow Pydantic's native selection: a supplied `"42"` remains a string
for `int | str`; an `int` field converts it to integer `42`.

## Downstream regression coverage

The [downstream regression coverage map](envblock-downstream-regression-coverage.md)
accounts for every captured definition and usage, with behaviour-focused offline
regressions for the observed EnvBlock contracts. It covers field conversion,
defaults, prefixes, secrets, constraints, saved loading, caching and dependent
consumer behavior. Native union selection and date/duration parsing are intentional
conversion changes; the map records the evidence boundaries and independent local
implementation exclusions.

## Progress

- [x] Replace manual type conversion with native Pydantic validation.
- [x] Keep existing prefixes, defaults, saved-block loading and caching.
- [x] Cover boolean parsing and existing downstream field shapes with offline tests.

## Next Steps

- Review and release the migration, then update the consuming application dependency.

## Blockers & Risks

- No downstream declaration changes are required for the audited implementations.
