import pytest

from mad_prefect.data_assets import asset
from mad_prefect.data_assets.data_asset import DataAsset


async def test_with_arguments_formats_templates_on_init():
    """Ensure placeholder values resolve as soon as with_arguments derives an asset."""

    @asset(
        path="bronze/{endpoint}.parquet",
        artifacts_dir="raw/{endpoint}",
        name="{endpoint}",
    )
    async def base(endpoint: str):
        return [{"endpoint": endpoint}]

    # Create a derived asset and verify formatting happens immediately, before awaiting execution.
    derived = base.with_arguments("widgets")
    assert derived.path == "bronze/widgets.parquet"
    assert derived.name == "widgets"
    assert derived.options.artifacts_dir == "raw/widgets"


async def test_with_arguments_isolates_data_asset_options():
    """Validate that two derivatives no longer share the same DataAssetOptions instance."""

    @asset(
        path="bronze/{endpoint}.parquet",
        artifacts_dir="raw/{endpoint}",
        name="{endpoint}",
    )
    async def base(endpoint: str):
        return [{"endpoint": endpoint}]

    # Two derived assets should format independently and not leak artifacts_dir mutations.
    dockets = base.with_arguments("dockets")
    fields = base.with_arguments("fields")

    await dockets()
    assert dockets.options.artifacts_dir == "raw/dockets"
    assert fields.options.artifacts_dir == "raw/fields"

    await fields()
    assert dockets.options.artifacts_dir == "raw/dockets"
    assert fields.options.artifacts_dir == "raw/fields"


async def test_with_options_updates_templates_for_future_derivatives():
    """Ensure overrides via with_options become the source templates for later derivatives."""

    @asset(
        path="bronze/{endpoint}.parquet",
        artifacts_dir="raw/{endpoint}",
        name="{endpoint}",
    )
    async def base(endpoint: str):
        return [{"endpoint": endpoint}]

    # Override templates using with_options so future with_arguments calls use custom formats.
    configured = base.with_options(
        path="silver/{endpoint}/data.parquet",
        artifacts_dir="processed/{endpoint}",
        name="silver-{endpoint}",
    )

    derived = configured.with_arguments("widgets")
    assert derived.path == "silver/widgets/data.parquet"
    assert derived.name == "silver-widgets"
    assert derived.options.artifacts_dir == "processed/widgets"


async def test_callable_formatting():
    @asset(
        path="{customer}/{endpoint}.parquet",
        name="{customer}-{endpoint}",
    )
    async def base(endpoint: str, customer: str):
        return {customer: endpoint}

    partial_asset = base.with_arguments(customer="test")
    await partial_asset(endpoint="lists")

    assert partial_asset.path == "test/lists.parquet"
    assert partial_asset.name == "test-lists"


async def test_partial_initialization():
    @asset(
        path="{customer}/{listing_asset.name}_details.parquet",
        name="{customer}-{listing_asset.name}-details",
    )
    async def detail_asset(listing_asset: DataAsset, customer: str):
        return {customer: listing_asset.name}

    partial_detail_asset = detail_asset.with_arguments(customer="ABC")

    assert partial_detail_asset.path == "ABC/{listing_asset.name}_details.parquet"
    assert partial_detail_asset.name == "ABC-{listing_asset.name}-details"



async def test_initialization_handles_missing_artifacts_dir_template():
    """assets with literal or None artifacts_dir shouldn't break partial formatting."""

    @asset(path="bronze/{endpoint}.parquet", artifacts_dir=None, name="{endpoint}")
    async def base(endpoint: str):
        return [{"endpoint": endpoint}]

    derived = base.with_arguments("widgets")
    assert derived.options.artifacts_dir == ""

    configured = base.with_options(artifacts_dir="static/output")
    configured_with_args = configured.with_arguments("widgets")
    assert configured_with_args.options.artifacts_dir == "static/output"


async def test_nested_asset_placeholders_survive_partial_pass():
    """Parent assets referencing nested derivatives should keep unresolved placeholders."""

    @asset(path="{segment}/{region}.parquet", name="{segment}-{region}")
    async def child(segment: str, region: str):
        return [segment, region]

    @asset(path="combo/{child_asset.path}", name="combo-{child_asset.name}")
    async def parent(child_asset: DataAsset):
        return child_asset

    partial_child = child.with_arguments(segment="north")
    partial_parent = parent.with_arguments(child_asset=partial_child)

    assert partial_parent.path == "combo/north/{region}.parquet"
    assert partial_parent.name == "combo-north-{region}"

    with pytest.raises(KeyError):
        await partial_parent()


async def test_call_raises_when_missing_placeholder_after_partial():
    """Missing placeholders should raise during execution, not initialization."""

    @asset(path="{customer}/{endpoint}.parquet", name="{customer}-{endpoint}")
    async def base(customer: str, endpoint: str):
        return customer, endpoint

    partial_asset = base.with_arguments(customer="acme")

    with pytest.raises(KeyError):
        await partial_asset()
