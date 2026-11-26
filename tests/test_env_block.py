import os
from datetime import datetime, timedelta, timezone
from typing import Optional, Union
import pytest
from pydantic import Field, SecretStr, ValidationError
from mad_prefect.envblock import EnvBlock


class DownstreamAPI(EnvBlock):
    token: str
    url: str
    retries: int = 3
    secret: SecretStr
    _internal: str = "should not load"


def reset_env_and_instance(cls):
    keys = [
        "DOWNSTREAMAPI_TOKEN",
        "DOWNSTREAMAPI_URL",
        "DOWNSTREAMAPI_SECRET",
        "DOWNSTREAMAPI_RETRIES",
        "DOWNSTREAMAPI_CREDENTIAL_BLOCK_NAME",
        "DOWNSTREAMAPI__INTERNAL",
    ]
    for key in keys:
        os.environ.pop(key, None)
    cls._instance = None


async def test_loads_env_values():
    DownstreamAPI._instance = None
    test_env = {
        "DOWNSTREAMAPI_TOKEN": "abc",
        "DOWNSTREAMAPI_URL": "https://x.com",
        "DOWNSTREAMAPI_SECRET": "shhh",
    }

    creds = await DownstreamAPI.from_env(test_env)
    assert creds.token == "abc"
    assert creds.url == "https://x.com"
    assert isinstance(creds.secret, SecretStr)
    assert creds.secret.get_secret_value() == "shhh"


async def test_uses_default_value():
    DownstreamAPI._instance = None
    test_env = {
        "DOWNSTREAMAPI_TOKEN": "abc",
        "DOWNSTREAMAPI_URL": "https://x.com",
        "DOWNSTREAMAPI_SECRET": "shhh",
    }

    creds = await DownstreamAPI.from_env(test_env)
    assert creds.retries == 3


async def test_missing_required_field_raises():
    DownstreamAPI._instance = None
    test_env = {
        "DOWNSTREAMAPI_TOKEN": "abc",
        "DOWNSTREAMAPI_SECRET": "shhh",
    }

    with pytest.raises(ValueError, match="DOWNSTREAMAPI_URL is required"):
        await DownstreamAPI.from_env(test_env)


async def test_skips_internal_fields():
    DownstreamAPI._instance = None
    test_env = {
        "DOWNSTREAMAPI_TOKEN": "abc",
        "DOWNSTREAMAPI_URL": "https://x.com",
        "DOWNSTREAMAPI_SECRET": "shhh",
        "DOWNSTREAMAPI__INTERNAL": "hacked",
    }

    block = await DownstreamAPI.from_env(test_env)
    assert block.token == "abc"
    assert block._internal == "should not load"  # not overridden


async def test_instance_cached():
    DownstreamAPI._instance = None

    test_env_a = {
        "DOWNSTREAMAPI_TOKEN": "a",
        "DOWNSTREAMAPI_URL": "a",
        "DOWNSTREAMAPI_SECRET": "a",
    }

    test_env_b = {
        "DOWNSTREAMAPI_TOKEN": "b",
        "DOWNSTREAMAPI_URL": "b",
        "DOWNSTREAMAPI_SECRET": "b",
    }

    a = await DownstreamAPI.from_env(test_env_a)
    b = await DownstreamAPI.from_env(test_env_b)
    assert a is b


async def test_loads_block_if_env_name_provided():
    DownstreamAPI._instance = None

    class FakeBlock(DownstreamAPI):
        @classmethod
        async def load(cls, name):
            return cls(token="loaded", url="https://loaded", secret=SecretStr("lol"))

    test_env = {"FAKEBLOCK_CREDENTIAL_BLOCK_NAME": "something"}

    result = await FakeBlock.from_env(test_env)
    assert result.token == "loaded"


async def test_explicit_prefix_loads_named_credential_block():
    class CredentialBlockPrefix(EnvBlock):
        prefix: str | None = "CustomPrefix"
        token: str

        @classmethod
        async def load(cls, name):
            assert name == "saved-credentials"
            return cls(token="loaded")

    test_env = {"CUSTOMPREFIX_CREDENTIAL_BLOCK_NAME": "saved-credentials"}

    result = await CredentialBlockPrefix.from_env(test_env)

    assert result.token == "loaded"


async def test_instances_are_isolated_per_subclass():
    class A(EnvBlock):
        token: str

    class B(EnvBlock):
        token: str

    reset_env_and_instance(A)
    reset_env_and_instance(B)

    os.environ["A_TOKEN"] = "aaa"
    os.environ["B_TOKEN"] = "bbb"

    a = await A.from_env()
    b = await B.from_env()

    assert a.token == "aaa"
    assert b.token == "bbb"
    assert a is not b


async def test_subclass_with_explicit_prefix():
    class ConfiguredPrefix(EnvBlock):
        prefix: str | None = "CustomPrefix"
        token: str

    test_env = {
        "CUSTOMPREFIX_TOKEN": "expected",
        "CONFIGUREDPREFIX_TOKEN": "should_be_ignored",
    }

    block = await ConfiguredPrefix.from_env(test_env)

    assert block.token == "expected"
    assert block.prefix == "CustomPrefix"
    assert ConfiguredPrefix.resolve_prefix() == "CUSTOMPREFIX"


def test_prefix_is_in_schema_and_serialized():
    schema = DownstreamAPI.model_json_schema()
    block = DownstreamAPI(
        token="abc",
        url="https://x.com",
        secret=SecretStr("shhh"),
    )

    assert schema["properties"]["prefix"]["default"] is None
    assert block.model_dump()["prefix"] is None


async def test_successful_casting_of_standard_type():
    DownstreamAPI._instance = None
    test_env = {
        "DOWNSTREAMAPI_TOKEN": "abc",
        "DOWNSTREAMAPI_URL": "https://x.com",
        "DOWNSTREAMAPI_SECRET": "shhh",
        "DOWNSTREAMAPI_RETRIES": "7",
    }

    block = await DownstreamAPI.from_env(test_env)
    assert block.retries == 7
    assert isinstance(block.retries, int)


async def test_unsuccessful_casting_raises_value_error():
    DownstreamAPI._instance = None
    test_env = {
        "DOWNSTREAMAPI_TOKEN": "abc",
        "DOWNSTREAMAPI_URL": "https://x.com",
        "DOWNSTREAMAPI_SECRET": "shhh",
        "DOWNSTREAMAPI_RETRIES": "notanint",
    }

    with pytest.raises(ValidationError) as error:
        await DownstreamAPI.from_env(test_env)

    assert error.value.errors()[0]["loc"] == ("retries",)
    assert error.value.errors()[0]["type"] == "int_parsing"


async def test_secretstr_field_is_wrapped_correctly():
    reset_env_and_instance(DownstreamAPI)
    os.environ["DOWNSTREAMAPI_TOKEN"] = "abc"
    os.environ["DOWNSTREAMAPI_URL"] = "https://x.com"
    os.environ["DOWNSTREAMAPI_SECRET"] = "supersecret"

    block = await DownstreamAPI.from_env()
    assert isinstance(block.secret, SecretStr)
    assert block.secret.get_secret_value() == "supersecret"


async def test_union_fields_use_pydantic_validation():
    class UnionBlock(EnvBlock):
        prefix: str | None = "UnionTest"
        retries: int | str  # Union[int, str]
        region: None | str  # Optional[str]
        token: Optional[str]
        url: Union[str, int]
        secret: SecretStr

    test_env = {
        "UNIONTEST_RETRIES": "42",  # already matches the string member
        "UNIONTEST_REGION": "au-east",  # should remain as str
        "UNIONTEST_TOKEN": "abc",
        "UNIONTEST_URL": "https://x.com",
        "UNIONTEST_SECRET": "shhh",
    }

    block = await UnionBlock.from_env(test_env)

    assert isinstance(block.retries, str)
    assert block.retries == "42"

    assert isinstance(block.region, str)
    assert block.region == "au-east"


@pytest.mark.parametrize("value, expected", [("false", False), ("true", True)])
async def test_backfill_environment_value_is_parsed_as_boolean(value, expected):
    class MetadataConfig(EnvBlock):
        prefix: str | None = "PREFECT_METADATA"
        backfill: bool = True

    config = await MetadataConfig.from_env({"PREFECT_METADATA_BACKFILL": value})

    assert config.backfill is expected


async def test_invalid_boolean_is_not_cached():
    class MetadataConfig(EnvBlock):
        backfill: bool = True

    with pytest.raises(ValidationError):
        await MetadataConfig.from_env({"METADATACONFIG_BACKFILL": "invalid"})

    assert MetadataConfig._instance is None
    config = await MetadataConfig.from_env({"METADATACONFIG_BACKFILL": "false"})
    assert config.backfill is False


@pytest.mark.parametrize("secret", [None, "synthetic-secret"])
async def test_optional_secret_can_be_omitted_or_loaded(secret):
    class OptionalCredentials(EnvBlock):
        token: SecretStr | None = None

    env = {} if secret is None else {"OPTIONALCREDENTIALS_TOKEN": secret}
    credentials = await OptionalCredentials.from_env(env)

    if secret is None:
        assert credentials.token is None
    else:
        assert isinstance(credentials.token, SecretStr)
        assert credentials.token.get_secret_value() == secret


async def test_date_and_duration_strings_use_pydantic_validation():
    class SurveyConfig(EnvBlock):
        last_delta_run: datetime | None = None
        cache_expiration: timedelta = timedelta(hours=1)

    config = await SurveyConfig.from_env(
        {
            "SURVEYCONFIG_LAST_DELTA_RUN": "2026-11-01T12:00:00Z",
            "SURVEYCONFIG_CACHE_EXPIRATION": "PT5M",
        }
    )

    assert config.last_delta_run == datetime(2026, 11, 1, 12, tzinfo=timezone.utc)
    assert config.cache_expiration == timedelta(minutes=5)


async def test_model_field_constraints_still_apply():
    class MetadataConfig(EnvBlock):
        window_days: int = Field(default=1, gt=0)

    with pytest.raises(ValidationError) as error:
        await MetadataConfig.from_env({"METADATACONFIG_WINDOW_DAYS": "0"})

    assert error.value.errors()[0]["loc"] == ("window_days",)
    assert error.value.errors()[0]["type"] == "greater_than"



async def test_plain_string_prefix_loads_named_credential_block():
    class PlainStringCredentialPrefix(EnvBlock):
        prefix: str = "CustomPrefix"  # pyright: ignore[reportIncompatibleVariableOverride]
        token: str

        @classmethod
        async def load(cls, name):
            assert name == "saved-credentials"
            return cls(token="loaded")

    test_env = {"CUSTOMPREFIX_CREDENTIAL_BLOCK_NAME": "saved-credentials"}

    result = await PlainStringCredentialPrefix.from_env(test_env)

    assert result.token == "loaded"


async def test_subclass_with_plain_string_prefix():
    class PlainStringConfiguredPrefix(EnvBlock):
        prefix: str = "CustomPrefix"  # pyright: ignore[reportIncompatibleVariableOverride]
        token: str

    test_env = {
        "CUSTOMPREFIX_TOKEN": "expected",
        "PLAINSTRINGCONFIGUREDPREFIX_TOKEN": "should_be_ignored",
    }

    block = await PlainStringConfiguredPrefix.from_env(test_env)

    assert block.token == "expected"
    assert block.prefix == "CustomPrefix"
    assert PlainStringConfiguredPrefix.resolve_prefix() == "CUSTOMPREFIX"
