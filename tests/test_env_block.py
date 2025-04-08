import os
import pytest
from pydantic import SecretStr
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
    reset_env_and_instance(DownstreamAPI)
    os.environ["DOWNSTREAMAPI_TOKEN"] = "abc"
    os.environ["DOWNSTREAMAPI_URL"] = "https://x.com"
    os.environ["DOWNSTREAMAPI_SECRET"] = "shhh"

    creds = await DownstreamAPI.from_env()
    assert creds.token == "abc"
    assert creds.url == "https://x.com"
    assert creds.retries == 3
    assert isinstance(creds.secret, SecretStr)
    assert creds.secret.get_secret_value() == "shhh"


async def test_uses_default_value():
    reset_env_and_instance(DownstreamAPI)
    os.environ["DOWNSTREAMAPI_TOKEN"] = "abc"
    os.environ["DOWNSTREAMAPI_URL"] = "https://x.com"
    os.environ["DOWNSTREAMAPI_SECRET"] = "s3cr3t"

    creds = await DownstreamAPI.from_env()
    assert creds.retries == 3


async def test_missing_required_field_raises():
    reset_env_and_instance(DownstreamAPI)
    os.environ["DOWNSTREAMAPI_TOKEN"] = "abc"
    os.environ["DOWNSTREAMAPI_SECRET"] = "s3cr3t"

    with pytest.raises(ValueError, match="DOWNSTREAMAPI_URL is required"):
        await DownstreamAPI.from_env()


async def test_skips_internal_fields():
    reset_env_and_instance(DownstreamAPI)
    os.environ["DOWNSTREAMAPI_TOKEN"] = "abc"
    os.environ["DOWNSTREAMAPI_URL"] = "https://x.com"
    os.environ["DOWNSTREAMAPI_SECRET"] = "shh"
    os.environ["DOWNSTREAMAPI__INTERNAL"] = "hacked"

    block = await DownstreamAPI.from_env()
    assert block.token == "abc"
    assert block._internal == "should not load"  # not overridden


async def test_instance_cached():
    reset_env_and_instance(DownstreamAPI)
    os.environ["DOWNSTREAMAPI_TOKEN"] = "abc"
    os.environ["DOWNSTREAMAPI_URL"] = "url"
    os.environ["DOWNSTREAMAPI_SECRET"] = "s"

    a = await DownstreamAPI.from_env()
    b = await DownstreamAPI.from_env()
    assert a is b


async def test_loads_block_if_env_name_provided():
    class FakeBlock(DownstreamAPI):
        @classmethod
        async def load(cls, name):
            return cls(token="loaded", url="https://loaded", secret=SecretStr("lol"))

    reset_env_and_instance(FakeBlock)
    os.environ["FAKEBLOCK_CREDENTIAL_BLOCK_NAME"] = "something"

    result = await FakeBlock.from_env()
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
        prefix: str = "CustomPrefix"
        token: str

    reset_env_and_instance(ConfiguredPrefix)
    os.environ["CUSTOMPREFIX_TOKEN"] = "expected"
    os.environ["CONFIGUREDPREFIX_TOKEN"] = "should_be_ignored"

    block = await ConfiguredPrefix.from_env()
    assert block.token == "expected"
