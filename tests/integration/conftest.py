"""Local storage for the decorated asset example; no configured services are used."""

from collections.abc import Iterator
from pathlib import Path

import duckdb
import pytest

from mad_prefect import duckdb as mad_duckdb
from mad_prefect import filesystems


@pytest.fixture
def local_asset_storage(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    filesystem = filesystems.FsspecFileSystem(basepath=tmp_path.as_uri())
    monkeypatch.setattr(filesystems, "_get_fs_result", filesystem)

    previous = mad_duckdb._mad_filesystem_ref
    was_registered = duckdb.filesystem_is_registered("mad")
    if was_registered:
        duckdb.unregister_filesystem("mad")
    monkeypatch.setattr(mad_duckdb, "_mad_filesystem_ref", None)

    try:
        yield tmp_path
    finally:
        if duckdb.filesystem_is_registered("mad"):
            duckdb.unregister_filesystem("mad")
        if was_registered and previous is not None:
            duckdb.register_filesystem(previous)
