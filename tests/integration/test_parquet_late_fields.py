"""Offline round trips through the real MAD.Prefect artifact writer."""

import json
from pathlib import Path
from unittest.mock import AsyncMock

import pyarrow.parquet as pq
import pytest

from mad_prefect.data_assets import data_artifact
from mad_prefect.data_assets.data_artifact import DataArtifact
from mad_prefect.filesystems import FsspecFileSystem


@pytest.fixture
def local_storage(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    filesystem = FsspecFileSystem(basepath=tmp_path.as_uri())
    monkeypatch.setattr(data_artifact, "get_fs", AsyncMock(return_value=filesystem))
    # Dictionary inputs do not use DuckDB; avoid registering configured storage.
    monkeypatch.setattr(data_artifact, "register_mad_protocol", AsyncMock())
    return tmp_path


@pytest.mark.parametrize("yield_batch", [False, True], ids=["list", "generator"])
@pytest.mark.parametrize(
    "include_field_in_first_row",
    [
        pytest.param(
            False,
            marks=pytest.mark.xfail(
                strict=True,
                raises=AssertionError,
                reason="Parquet conversion drops keys absent from the first dictionary",
            ),
            id="late-field",
        ),
        pytest.param(True, id="explicit-null-control"),
    ],
)
async def test_parquet_preserves_fields_from_later_rows(
    local_storage: Path, yield_batch: bool, include_field_in_first_row: bool
) -> None:
    first_row: dict[str, object] = {"id": 1}
    if include_field_in_first_row:
        first_row["extra"] = None
    rows = [first_row, {"id": 2, "extra": "keep me"}]

    def batches():
        yield rows

    artifact = DataArtifact("late-field.parquet", batches() if yield_batch else rows)
    assert await artifact.persist() is True

    # Read the physical file, rather than inspecting an intermediate Arrow object.
    actual = pq.read_table(local_storage / "late-field.parquet").to_pylist()
    assert actual == [{"id": 1, "extra": None}, {"id": 2, "extra": "keep me"}]


async def test_json_preserves_fields_from_later_rows(local_storage: Path) -> None:
    rows = [{"id": 1}, {"id": 2, "extra": "keep me"}]
    artifact = DataArtifact("late-field.json", rows)
    assert await artifact.persist() is True

    actual = [
        json.loads(line)
        for line in (local_storage / "late-field.json").read_text().splitlines()
    ]
    assert actual == rows
