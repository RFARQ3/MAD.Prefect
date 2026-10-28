"""Run this file directly to see the sample input and queried asset output."""

import asyncio
from pathlib import Path
from tempfile import TemporaryDirectory

from mad_prefect import filesystems
from mad_prefect.data_assets import asset


SAMPLE_DATA = [
    {"id": 1},
    {"id": 2, "extra": "keep me"},
]


@asset("late_fields.parquet", artifact_filetype="parquet")
def sample_asset():
    return SAMPLE_DATA


async def show_results() -> list[dict[str, object]]:
    result = await sample_asset.query()
    assert result is not None
    output = [dict(zip(result.columns, row)) for row in result.fetchall()]

    print("Input:", SAMPLE_DATA)
    print("Output:", output)
    print("Late field missing:", "extra" not in result.columns)
    return output


async def test_asset_loses_late_field(local_asset_storage: Path) -> None:
    output = await show_results()

    # This demonstrates the current bug, not the behavior we want to keep.
    assert SAMPLE_DATA[1]["extra"] == "keep me"
    assert [row["id"] for row in output] == [1, 2]
    assert "extra" not in output[1]


if __name__ == "__main__":
    # Keep this demonstration local, independent of configured storage or services.
    with TemporaryDirectory(prefix="mad-prefect-late-fields-") as directory:
        filesystems._get_fs_result = filesystems.FsspecFileSystem(
            basepath=Path(directory).as_uri()
        )
        asyncio.run(show_results())
