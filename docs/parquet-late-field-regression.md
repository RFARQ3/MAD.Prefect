# Parquet late-field regression

Run the offline integration test:

```powershell
poetry run pytest tests/integration/test_parquet_late_fields.py -rx
```

To expose the failing preservation assertions directly:

```powershell
poetry run pytest tests/integration/test_parquet_late_fields.py --runxfail
```

The input is `[{"id": 1}, {"id": 2, "extra": "keep me"}]`. The expected
Parquet output retains `extra`, with null in the first row. The current writer
returns success but writes only `id`. Both a direct list and a generator yielding
that list exercise the real `DataArtifact.persist()` implementation and read the
resulting Parquet file back through PyArrow.

Only configured storage lookup and unused DuckDB protocol registration are
replaced. The test uses the real FsspecFileSystem with pytest temporary storage;
no customer data, Prefect API, or remote filesystem is used. Conversion, batch
iteration, sanitization, Parquet writing, and readback are not mocked.

## Progress

- Regression and controls added; late-field cases are strict expected failures.
- Explicit null in the first dictionary and JSON writing are preservation controls.
- Production behavior is unchanged.

## Next Steps

Fix dictionary-to-Arrow conversion separately, then remove the expected-failure
marks. A passing regression produces a strict XPASS until those marks are removed.

## Blockers & Risks

The test covers top-level fields within one batch. It does not establish behavior
for schema changes between batches or nested fields. Expected-failure marks accept
only assertion failures; `--runxfail` shows the exact failure for review.

## Validation

Verified with Python 3.11.9, PyArrow 16.1.0, DuckDB 1.4.3, and Prefect 3.4.25
in an isolated environment (the fork has no configured Poetry environment).
Normal run: 3 passed, 2 expected failures. With `--runxfail`: 3 passed, 2 failed,
both at the file-content assertion because `extra` is absent. Pyright basic:
0 errors. Whitespace checks: no errors.

The existing full suite was not run: it uses configured storage and framework
execution outside this test's isolated filesystem fixture. This change verifies
only the requested offline integration cases.

Suggested commit: `test: reproduce parquet loss of fields absent from first row`

## Simple decorated asset example

`tests/integration/test_asset_late_fields_example.py` contains a global sample,
a decorated function that returns it, and a test confirming that the saved output
is missing `extra`. Run with `poetry run pytest -s tests/integration/test_asset_late_fields_example.py`
to print input, queried output, and whether the late field is missing. The separate conftest fixture
routes the whole asset pipeline to temporary local storage. The asset pipeline
also adds its normal `asset` column to the output.

This example deliberately asserts the observed bug; the original regression test
continues to assert the desired preservation behavior.

Open the example in the editor and press F5 to run its main block with the
project Python interpreter. It calls sample_asset.query() and uses temporary
local storage. The standalone run prints: Late field missing: True.
The combined integration directory reports 4 passed and 2 expected failures.
