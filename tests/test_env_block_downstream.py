"""Distinct EnvBlock behaviours observed in downstream consumers."""

from datetime import datetime, timedelta, timezone
import json
from typing import Optional
from unittest.mock import AsyncMock

import httpx
from pydantic import Field, PrivateAttr, SecretStr, ValidationError
import pytest

from mad_prefect.envblock import EnvBlock


class ConsumerConfig(EnvBlock, httpx.Auth):
    prefix: str | None = "CONSUMER"
    token: SecretStr
    label: str = "default"
    retries: int = 3
    timeout: float = 60.0
    enabled: bool = True
    since: datetime | None = None
    expiration: timedelta = timedelta(hours=1)
    window: int = Field(default=1, gt=0)
    _tokens: dict = PrivateAttr(default_factory=dict)


@pytest.fixture(autouse=True)
def reset_consumer_cache():
    ConsumerConfig._instance = None
    yield
    ConsumerConfig._instance = None


async def test_consumer_defaults_and_auth_mixin():
    config = await ConsumerConfig.from_env({"CONSUMER_TOKEN": "synthetic"})
    assert isinstance(config, httpx.Auth)
    assert config.token.get_secret_value() == "synthetic"
    assert (config.label, config.retries, config.timeout, config.enabled) == ("default", 3, 60.0, True)
    assert config.since is None
    assert config.expiration == timedelta(hours=1)
    assert config.window == 1
    assert config._tokens == {}


async def test_numeric_strings_support_consumer_arithmetic():
    config = await ConsumerConfig.from_env({"CONSUMER_TOKEN": "synthetic", "CONSUMER_RETRIES": "7", "CONSUMER_TIMEOUT": "12.5"})
    assert config.retries == 7 and type(config.retries) is int
    assert config.timeout == 12.5 and type(config.timeout) is float
    assert list(range(config.retries)) == list(range(7))


@pytest.mark.parametrize("raw", ["", "  spaced value  ", "é/東京/🙂", '{"project":"synthetic-token"}', "false", "0"])
async def test_text_and_secrets_preserve_exact_input(raw):
    config = await ConsumerConfig.from_env({"CONSUMER_TOKEN": raw, "CONSUMER_LABEL": raw})
    assert config.label == raw
    assert isinstance(config.token, SecretStr)
    assert config.token.get_secret_value() == raw
    assert not raw or raw not in repr(config.token)


@pytest.mark.parametrize("field", ["retries", "timeout", "enabled", "since", "expiration"])
async def test_invalid_typed_value_does_not_cache_and_can_recover(field):
    values = {"CONSUMER_TOKEN": "synthetic", f"CONSUMER_{field.upper()}": "invalid"}
    with pytest.raises(ValidationError) as error:
        await ConsumerConfig.from_env(values)
    assert error.value.errors()[0]["loc"] == (field,)
    assert ConsumerConfig._instance is None
    assert (await ConsumerConfig.from_env({"CONSUMER_TOKEN": "synthetic"})).token.get_secret_value() == "synthetic"


@pytest.mark.parametrize("raw", ["0", "-1"])
async def test_positive_constraint_rejects_nonpositive_values(raw):
    with pytest.raises(ValidationError) as error:
        await ConsumerConfig.from_env({"CONSUMER_TOKEN": "synthetic", "CONSUMER_WINDOW": raw})
    assert error.value.errors()[0]["type"] == "greater_than"


async def test_wrong_case_or_prefix_cannot_supply_required_field():
    with pytest.raises(ValueError, match="CONSUMER_TOKEN is required"):
        await ConsumerConfig.from_env({"consumer_token": "wrong-case", "OTHER_TOKEN": "wrong-prefix"})
    assert ConsumerConfig._instance is None


async def test_private_mutable_state_is_ignored_and_isolated():
    first = await ConsumerConfig.from_env({"CONSUMER_TOKEN": "synthetic", "CONSUMER__TOKENS": "ignored"})
    first._tokens["project"] = "synthetic-token"
    assert "_tokens" not in first.model_dump()
    assert "_tokens" not in ConsumerConfig.model_fields
    ConsumerConfig._instance = None
    second = await ConsumerConfig.from_env({"CONSUMER_TOKEN": "synthetic"})
    assert second._tokens == {}
    assert second._tokens is not first._tokens


async def test_saved_load_failure_is_not_cached(monkeypatch):
    load = AsyncMock(side_effect=ValueError("missing block"))
    monkeypatch.setattr(ConsumerConfig, "load", load)
    with pytest.raises(ValueError, match="missing block"):
        await ConsumerConfig.from_env({"CONSUMER_CREDENTIAL_BLOCK_NAME": "missing"})
    assert ConsumerConfig._instance is None
    assert (await ConsumerConfig.from_env({"CONSUMER_TOKEN": "synthetic"})).token.get_secret_value() == "synthetic"


async def test_saved_load_takes_precedence_over_invalid_environment(monkeypatch):
    saved = ConsumerConfig(token=SecretStr("synthetic"), enabled=False)
    load = AsyncMock(return_value=saved)
    monkeypatch.setattr(ConsumerConfig, "load", load)
    assert await ConsumerConfig.from_env({"CONSUMER_CREDENTIAL_BLOCK_NAME": "saved", "CONSUMER_ENABLED": "invalid"}) is saved
    assert await ConsumerConfig.from_env({}) is saved
    load.assert_awaited_once_with("saved")


@pytest.mark.parametrize("raw, expected", [("FALSE", False), ("0", False), ("off", False), ("no", False), ("TRUE", True), ("1", True), ("on", True), ("yes", True)])
async def test_boolean_spellings_use_value_not_string_truthiness(raw, expected):
    config = await ConsumerConfig.from_env({"CONSUMER_TOKEN": "synthetic", "CONSUMER_ENABLED": raw})
    assert config.enabled is expected


@pytest.mark.parametrize("raw, expected", [(" first SERVICE20_TEXT_006 second ", ["first", "second"]), ("", [""]), (" same SERVICE20_TEXT_006same ", ["same", "same"])])
async def test_definition28_delimited_report_properties_keep_string_semantics(raw, expected):
    from test_envblock_downstream_usage import consumer_model

    model = consumer_model(28, {"report_guid_list", "snapshot_report_guids_list"})
    config = await model.from_env({
        "SERVICE20_TEXT_001_BASE_URL": "https://service.invalid",
        "SERVICE20_TEXT_001_API_TOKEN": "synthetic-token",
        "SERVICE20_TEXT_001_REPORT_GUIDS": raw,
        "SERVICE20_TEXT_001_SNAPSHOT_REPORT_GUIDS": raw,
    })
    assert config.report_guid_list == expected
    assert config.snapshot_report_guids_list == expected


@pytest.mark.parametrize("raw, keys, token", [('{"one":"synthetic-token","two":"other"}', ["one", "two"], "synthetic-token"), ("{}", [], ""), ("null", [], "")])
async def test_definition42_json_secret_drives_project_keys_token_and_headers(raw, keys, token):
    from types import SimpleNamespace
    from test_envblock_downstream_usage import consumer_model

    model = consumer_model(42, {"get_token", "get_project_keys", "apply_auth"}, json=json)
    config = await model.from_env({"SERVICE28_TEXT_002_PROJECT_JSON_STR": raw})
    assert config.project_json_str.get_secret_value() == raw
    assert config.get_project_keys() == keys
    assert config.get_token("one").get_secret_value() == token
    assert config.get_token("absent").get_secret_value() == ""
    client = SimpleNamespace(headers={})
    config.apply_auth(client, "one")
    assert client.headers == {"SERVICE28_TEXT_004": "SERVICE28_TEXT_005" + token}


@pytest.mark.parametrize("raw", ["not-json", '"not-an-object"', "[1,2]"])
async def test_definition42_invalid_json_is_rejected_by_consumer(raw):
    from test_envblock_downstream_usage import consumer_model

    model = consumer_model(42, {"get_token", "get_project_keys"}, json=json)
    config = await model.from_env({"SERVICE28_TEXT_002_PROJECT_JSON_STR": raw})
    with pytest.raises(ValueError):
        config.get_token("one")
    with pytest.raises(ValueError):
        config.get_project_keys()


@pytest.mark.parametrize("definition_id, prefix, marker, placeholder", [(24, "SERVICE16_TEXT_002", "SERVICE16_TEXT_003", "company"), (25, "SERVICE17_TEXT_002", "SERVICE17_TEXT_004", "company_name")])
@pytest.mark.parametrize("templated", [False, True])
async def test_definitions24_25_company_url_templates_receive_unchanged_strings(definition_id, prefix, marker, placeholder, templated):
    from test_envblock_downstream_usage import consumer_model

    model = consumer_model(definition_id, {"resolve_base_url"}, PrivateAttr=PrivateAttr, Optional=Optional)
    base_url = f"https://service.invalid/{marker}/{{{placeholder}}}" if templated else "https://service.invalid/plain"
    environment = {
        f"{prefix}_BASE_URL": base_url,
        f"{prefix}_COMPANY": "synthetic-company",
        f"{prefix}_TOKEN": "synthetic-token",
        f"{prefix}_USERNAME": "synthetic-user",
        f"{prefix}_PASSWORD": "synthetic-password",
    }
    config = await model.from_env(environment)
    expected = base_url.format(**{placeholder: "synthetic-company"}) if templated else base_url
    assert config.resolve_base_url() == expected
    assert config.base_url == base_url
