"""Exercise observed consumer methods with synthetic inputs and offline collaborators."""

import ast
import json
import os
from uuid import uuid4
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from pydantic import SecretStr

from mad_prefect.envblock import EnvBlock


# Only synthetic examples required by these behaviours are shipped with the tests.
DOCUMENT = Path(__file__).parents[0] / "fixtures/envblock_consumer_examples.json"
EXAMPLES = json.loads(DOCUMENT.read_text(encoding="utf-8"))


def excerpt(kind, number):
    return EXAMPLES[f"{kind} {number}"]


def consumer_model(number, methods, **collaborators):
    """Keep audited fields and selected method bodies; replace external SDK bases."""
    tree = ast.parse(excerpt("Definition", number))
    definition = tree.body[0]
    assert isinstance(definition, ast.ClassDef)
    definition.bases = [ast.Name(id="EnvBlock", ctx=ast.Load())]
    definition.body = [
        node for node in definition.body
        if isinstance(node, ast.AnnAssign)
        or isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in methods
    ]
    definition.body.append(ast.Assign(targets=[ast.Name(id="_block_type_slug", ctx=ast.Store())], value=ast.Constant(value=f"usage-{number}-{uuid4().hex}")))
    namespace = {
        "__name__": __name__,
        "EnvBlock": EnvBlock, "SecretStr": SecretStr, "datetime": datetime,
        "timedelta": timedelta, "os": os, **collaborators,
    }
    tree.body.insert(0, ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0))
    exec(compile(ast.fix_missing_locations(tree), str(DOCUMENT), "exec"), namespace)
    model = namespace[definition.name]
    model._instance = None
    return model


def consumer_function(number, **collaborators):
    tree = ast.parse(excerpt("Usage", number))
    function = tree.body[0]
    assert isinstance(function, (ast.FunctionDef, ast.AsyncFunctionDef))
    function.decorator_list = []
    namespace = {"datetime": datetime, "timedelta": timedelta, **collaborators}
    tree.body.insert(0, ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0))
    exec(compile(ast.fix_missing_locations(tree), str(DOCUMENT), "exec"), namespace)
    return namespace[function.name]


@pytest.mark.parametrize("implicit", [False, True])
async def test_usage114_override_preserves_remapping_and_explicit_precedence(monkeypatch, implicit):
    model = consumer_model(9, {"from_env"})
    supplied = {"SERVICE08_TEXT_001": "legacy", "SERVICE08_TEXT_004SERVICE08_TEXT_001": "explicit"}
    captured = AsyncMock(return_value="loaded")
    monkeypatch.setattr(EnvBlock, "from_env", captured)
    monkeypatch.setattr(os, "environ", supplied.copy())
    assert await model.from_env(None if implicit else supplied) == "loaded"
    values = captured.call_args.args[0]
    assert values["SERVICE08_TEXT_004SERVICE08_TEXT_001"] == "explicit"
    assert supplied == {"SERVICE08_TEXT_001": "legacy", "SERVICE08_TEXT_004SERVICE08_TEXT_001": "explicit"}
    captured.reset_mock()
    await model.from_env({"SERVICE08_TEXT_002": "legacy-only"})
    assert captured.call_args.args[0]["SERVICE08_TEXT_004SERVICE08_TEXT_002"] == "legacy-only"


async def test_definition09_override_loads_real_model_and_calendar_values():
    model = consumer_model(9, {"from_env", "get_calendar_ids"})
    config = await model.from_env({
        "SERVICE08CONFIG01_SERVICE_ACCOUNT_FILE": "synthetic.json",
        "SERVICE08CONFIG01_CALENDAR_IDS": " a SERVICE08_TEXT_006bSERVICE08_TEXT_006aSERVICE08_TEXT_006 ",
    })
    assert config.service_account_file == "synthetic.json"
    assert config.impersonate_user is None
    assert config.get_calendar_ids() == ["a", "b"]
    config.calendar_ids = "   "
    with pytest.raises(ValueError):
        config.get_calendar_ids()


@pytest.mark.parametrize("protocol", ["SERVICE05_TEXT_009", "SERVICE05_TEXT_010", "SERVICE05_TEXT_011", "local"])
async def test_usage155_registration_and_cached_repeat(monkeypatch, protocol):
    register = AsyncMock()
    filesystem = AsyncMock(return_value=SimpleNamespace(basepath=f"{protocol}SERVICE05_TEXT_008directory"))
    database = SimpleNamespace(execute=Mock())
    model = consumer_model(6, {"register_env", "Service05Symbol005"},
                           register_mad_protocol=register, get_fs=filesystem, Service05Symbol006=database)
    monkeypatch.setattr(os, "environ", {
        "SERVICE05_TEXT_002_SHARED_KEY": "synthetic-shared",
        "SERVICE05_TEXT_002_SHARED_KEY_ID": "synthetic-id",
        "SERVICE05_TEXT_002_SERVICE05SYMBOL002": "synthetic-database",
    })
    config = await model.register_env()
    assert isinstance(config.Service05Symbol002, SecretStr)
    assert config.ingest_rolling_days == 1825
    assert config.Service05Symbol003 == 3
    if protocol == "local":
        database.execute.assert_not_called()
        assert config.Service05Symbol004 == "SERVICE05_TEXT_003"
    else:
        database.execute.assert_called_once_with("SERVICE05_TEXT_006synthetic-databaseSERVICE05_TEXT_007", [])
        assert config.Service05Symbol004 == "SERVICE05_TEXT_012directorySERVICE05_TEXT_013"
    assert await model.register_env() is config
    register.assert_awaited_once()
    filesystem.assert_awaited_once()


@pytest.mark.parametrize("definition", [40, 41])
@pytest.mark.parametrize("saved", [False, True])
async def test_definitions40_41_save_env_preserves_typed_updates(monkeypatch, definition, saved):
    logger = SimpleNamespace(warning=Mock())
    model = consumer_model(definition, {"save_env", "auth_flow"}, logger=logger)
    config = await model.from_env({"SERVICE27_TEXT_001_BASE_URL": "https://service.invalid", "SERVICE27_TEXT_001_TOKEN": "synthetic-token"})
    timestamp = datetime(2026, 11, 1, tzinfo=timezone.utc)
    config.last_delta_run = timestamp
    config.Service27Symbol002 = timestamp
    save = AsyncMock()
    monkeypatch.setattr(model, "save", save)
    monkeypatch.setattr(os, "environ", {"SERVICE27_TEXT_001_CREDENTIAL_BLOCK_NAME": "synthetic-block"} if saved else {})
    await config.save_env()
    if saved:
        save.assert_awaited_once_with(name="synthetic-block", overwrite=True)
        logger.warning.assert_not_called()
    else:
        save.assert_not_awaited()
        logger.warning.assert_called_once()
    assert (await model.from_env({})).last_delta_run == timestamp
    request = SimpleNamespace(headers={})
    assert list(config.auth_flow(request)) == [request]
    assert request.headers["SERVICE27_TEXT_004"] == "SERVICE27_TEXT_005synthetic-token"


@pytest.mark.parametrize("definition", [40, 41])
@pytest.mark.parametrize("mode, age", [("full", None), ("SERVICE27_TEXT_015", 1), ("SERVICE27_TEXT_014", 1), ("full", 1), ("full", 40)])
async def test_usage158_cache_expiration_branches_use_loaded_types(definition, mode, age):
    model = consumer_model(definition, set())
    config = await model.from_env({"SERVICE27_TEXT_001_BASE_URL": "https://service.invalid", "SERVICE27_TEXT_001_TOKEN": "synthetic-token"})
    runtime = datetime(2026, 11, 25, tzinfo=timezone.utc)
    config.Service27Symbol002 = None if age is None else runtime - timedelta(days=age)
    owner = SimpleNamespace(initialize=AsyncMock(return_value=SimpleNamespace(ingest_type=mode)))
    fetch = consumer_function(158, timezone=timezone, Service27Config01=model)
    result = await fetch(owner, mode, runtime)
    expected = config.Service27Symbol003
    if age is not None:
        expected = config.standard_cache_expiration if mode.startswith("SERVICE27_") else max(timedelta(days=age), config.Service27Symbol003)
    assert result == expected
    assert isinstance(result, timedelta)


@pytest.mark.parametrize("raw, backfill", [("false", False), ("true", True), ("0", False), ("1", True)])
async def test_usage129_backfill_controls_real_consumer_branch(raw, backfill):
    model = consumer_model(30, set())
    prefix = model.resolve_prefix()
    await model.from_env({f"{prefix}_BACKFILL": raw, f"{prefix}_TRAILING_DAYS": "2", f"{prefix}_PARTITION_HOURS": "36"})
    class Partition:
        def __init__(self, *args, **kwargs):
            assert kwargs["partition_hours"] == 36

        def validated_path(self, fs):
            return None

    materialize = AsyncMock()
    logs = AsyncMock()
    cleanup = AsyncMock()
    run = consumer_function(129, Service22Config01=model, UTC=timezone.utc, DatePartition=Partition,
                            materialize_log_partition=materialize, get_fs=AsyncMock(return_value=object()),
                            logs=logs, cleanup_partitions=cleanup)
    await run()
    assert materialize.await_count == (4 if backfill else 1)
    logs.assert_awaited_once_with([])
    cleanup.assert_awaited_once()



@pytest.mark.parametrize('definition, usage, symbol', [(10, 45, 'Service09Config01'), (3, 67, 'Service02Config01')])
async def test_client_wrapper_retains_cached_settings_and_mutations(definition, usage, symbol):
    model = consumer_model(definition, set())
    prefix = model.resolve_prefix()
    settings = await model.from_env({f'{prefix}_CLIENT_ID': 'synthetic-id', f'{prefix}_CLIENT_SECRET': 'synthetic-secret'})
    wrapper = consumer_function(usage, **{symbol: model})
    construct = lambda settings: SimpleNamespace(settings=settings)
    first = await wrapper(construct)
    second = await wrapper(construct)
    assert first.settings is second.settings is settings
    if definition == 3:
        first.settings.scopes = 'changed-scope'
        assert second.settings.scopes == 'changed-scope'
    assert first.settings.client_secret.get_secret_value() == 'synthetic-secret'


@pytest.mark.parametrize('definition', [45, 46])
@pytest.mark.parametrize('allowed', [False, True])
@pytest.mark.parametrize('delta', [False, True])
async def test_usages138_150_scopes_gate_work_and_integer_cache_minutes(definition, allowed, delta):
    from pydantic import Field
    model = consumer_model(definition, set(), Field=Field)
    prefix = model.resolve_prefix()
    await model.from_env({f'{prefix}_CLIENT_ID': 'synthetic-id', f'{prefix}_CLIENT_SECRET': 'synthetic-secret',
                          f'{prefix}_SCOPES': 'readSERVICE31_TEXT_017write', f'{prefix}_DELTA_CACHE_MINUTES': '5',
                          f'{prefix}_NORMAL_CACHE_MINUTES': '60'})
    factory = AsyncMock(return_value='materialized')
    owner = SimpleNamespace(required_scopes=['read'] if allowed else ['missing'], delta_func=delta,
                            factory=SimpleNamespace(name='synthetic'), configured_factory=AsyncMock(return_value=factory))
    configure = Mock(return_value=factory)
    logger = SimpleNamespace(info=Mock())
    should = consumer_function(150, Service31Config01=model)
    assert await should(owner) is allowed
    run = consumer_function(138, Service31Config01=model, configure_asset_cache=configure, logger=logger)
    assert await run(owner, 'argument', example=True) == ('materialized' if allowed else None)
    if allowed:
        configure.assert_called_once_with(factory, timedelta(minutes=5 if delta else 60))
        factory.assert_awaited_once_with('argument', example=True)
    else:
        configure.assert_not_called()
        owner.configured_factory.assert_not_awaited()


async def test_definition09_override_loads_and_caches_saved_block(monkeypatch):
    model = consumer_model(9, {'from_env'})
    saved = model(service_account_file='synthetic.json')
    load = AsyncMock(return_value=saved)
    monkeypatch.setattr(model, 'load', load)
    assert await model.from_env({'SERVICE08CONFIG01_CREDENTIAL_BLOCK_NAME': 'synthetic'}) is saved
    assert await model.from_env({}) is saved
    load.assert_awaited_once_with('synthetic')


@pytest.mark.parametrize('subject', [None, '', 'synthetic-user'])
async def test_definition09_credentials_forward_file_and_optional_subject(subject):
    credentials = SimpleNamespace(with_subject=Mock(return_value='delegated'))
    loader = Mock(return_value=credentials)
    model = consumer_model(9, {'from_env', 'get_credentials'},
                           Service08Symbol002=SimpleNamespace(Credentials=SimpleNamespace(from_service_account_file=loader)))
    values = {'SERVICE08CONFIG01_SERVICE_ACCOUNT_FILE': 'synthetic.json'}
    if subject is not None:
        values['SERVICE08CONFIG01_IMPERSONATE_USER'] = subject
    config = await model.from_env(values)
    assert config.get_credentials() == ('delegated' if subject else credentials)
    loader.assert_called_once_with('synthetic.json', scopes=['https://service.invalid/value-5'])
    if subject:
        credentials.with_subject.assert_called_once_with(subject)
    else:
        credentials.with_subject.assert_not_called()
