# Maintainer Guide

## System design

```
                            ┌─────────────────────┐
                            │   YAML test files    │
                            │  (given/sql/expect)  │
                            └──────────┬──────────┘
                                       │
                               ┌───────▼───────┐
                               │   models.py   │
                               │  Parse YAML → │
                               │  TestCase[]   │
                               └───────┬───────┘
                                       │
                        ┌──────────────▼──────────────┐
                        │     flink_sql_test.py        │
                        │                              │
                        │  1. Load tests               │
                        │  2. Lint checks (--lint)      │
                        │  3. Auto-detect backend      │
                        │  4. Dispatch to backend      │
                        │  5. Compare results          │
                        │  6. Print report             │
                        └──────┬───────────┬───────────┘
                               │           │
                   ┌───────────▼───┐   ┌───▼───────────────┐
                   │    DuckDB     │   │     PyFlink        │
                   │   Backend     │   │    Backend         │
                   │               │   │                    │
                   │ In-memory DB  │   │  filesystem/JSON   │
                   │ CREATE TEMP   │   │  connector for all │
                   │ TABLE+INSERT  │   │  tables (batch +   │
                   │ Execute SQL   │   │  streaming)        │
                   └───────┬───────┘   └────────┬───────────┘
                           │                    │
                           └────────┬───────────┘
                                    │
                              ┌─────▼──────┐
                              │comparator  │
                              │            │
                              │ Normalize  │
                              │ Sort       │
                              │ Diff       │
                              └────────────┘
```

## Module responsibilities

### `flink_sql_test.py` -- Orchestrator

The entry point. Owns the CLI, the test loop, and the output formatting.

Key decisions it makes:
- **Backend selection** (`detect_backend`, line 28): Regex scans the SQL for streaming constructs (`TUMBLE`, `HOP`, `SESSION`, `MATCH_RECOGNIZE`, `FOR SYSTEM_TIME AS OF`, `CUMULATE`). Also checks whether any input table declares a watermark. If either matches, routes to Flink; otherwise DuckDB.
- **Lazy init**: Backends are created on first use and cached in a `dict`. PyFlink is never imported unless a test actually needs it.
- **Graceful skip**: If a test needs Flink but `apache-flink` isn't installed, the `ImportError` is caught and the test is reported as `SKIP`, not `FAIL`.
- **CLI-level strict mode**: The `--strict` flag applies strict column projection to all tests. It is OR'd with the per-test `expect.strict` YAML setting in `run_test()`, so either the CLI flag or the YAML field can enable strict mode for a given test.
- **Lint integration**: The `--lint` flag imports from `linter.py` and runs `lint_test()` on each test before execution. Lint results (WARN/ERROR) are printed inline above the test result. Lint counts are tracked and shown in the summary. The linter import is lazy (only when `--lint` is passed), matching the pattern used for backend imports.

### `models.py` -- YAML parsing and data model

Defines the core data structures and handles all YAML deserialization.

```
TestCase
├── name: str
├── sql: str
├── backend: str | None          # None = auto-detect
├── given: list[TableInput]
│   ├── name: str
│   ├── rows: list[dict]
│   ├── schema: list[ColumnSchema] | None
│   ├── watermark: str | None
│   └── primary_key: list[str] | None
└── expect: ExpectedOutput
    ├── rows: list[dict]
    ├── ordered: bool
    └── strict: bool
```

**Two input formats** are supported:
1. **Shorthand** -- `given.tablename: [{col: val}, ...]` -- types are inferred from Python values via `_infer_type`.
2. **Explicit** -- `given.tablename: {schema: [...], watermark: "...", rows: [...]}` -- the schema block provides Flink SQL type strings and an optional watermark expression.

Type inference rules (`_infer_type`): `bool` -> BOOLEAN, `int` -> INT, `float` -> DOUBLE, `Decimal` -> DECIMAL(10,2), `datetime` -> TIMESTAMP(3), `date` -> DATE, everything else -> STRING.

### `comparator.py` -- Result comparison

Compares `list[dict]` actual vs expected. Four features to know about:

1. **Partial column matching** (default): If the expected row has fewer keys than the actual row, only the expected keys are compared. This is determined from `expected[0].keys()` and applied to every actual row via `_normalize_row(row, columns)`.

2. **Strict column projection** (`strict=True`): The actual output must have exactly the same columns as the expected rows -- no extra, no missing, and in the same order. Enabled per-test via `expect.strict: true` in YAML or globally via the `--strict` CLI flag. When strict mode is active, actual rows are normalized without column projection (all columns are kept).

3. **Order independence**: By default, both sides are sorted by `_row_sort_key` (alphabetical key-value tuples). Set `ordered: true` in the YAML to compare in the order given.

4. **Type coercion** (`_normalize_value`): Decimal -> float, datetime/date -> string, float rounded to 6 decimal places, strings stripped. This handles the mismatch between what DuckDB returns (Python native types) and what the YAML defines.

### `linter.py` -- SQL lint rules

Provides pre-execution lint checks for test SQL. SQLGlot is an optional dependency (lazy-imported via `try/except`, same pattern as PyFlink).

**Two categories of rules:**

1. **AST rules** (require sqlglot): Parse SQL with `sqlglot.parse(sql, error_level=sqlglot.ErrorLevel.WARN)` for best-effort parsing. SQLGlot has no native Flink dialect, so Flink-specific syntax (TUMBLE, HOP, temporal joins, MATCH_RECOGNIZE) is detected via `FLINK_SPECIFIC_PATTERN` regex and AST rules are skipped entirely for those queries to avoid false positives.

2. **Context rules** (no sqlglot needed): Use regex matching on the SQL text combined with `TestCase` metadata (watermarks, primary keys). These always run.

**`_parse_sql()`** is the gatekeeper for AST rules. It returns `None` (causing the rule to skip) if the SQL matches `FLINK_SPECIFIC_PATTERN`. It also temporarily sets the `sqlglot` logger to `CRITICAL` during parsing to suppress WARN-level messages that SQLGlot emits for unrecognized syntax.

**Rule functions** all have the signature `(test: TestCase) -> list[LintResult]` and are registered in `AST_RULES` or `CONTEXT_RULES` lists. The entry point `lint_test()` iterates both lists.

### `backends/base.py` -- Backend interface

A two-method ABC:

```python
class Backend(ABC):
    def execute_test(self, test: TestCase) -> list[dict]:  # required
    def cleanup(self):                                      # optional
```

Every backend must turn `TestCase.given` into tables, run `TestCase.sql`, and return the result as `list[dict]`.

### `backends/duckdb_backend.py` -- DuckDB backend

Single in-memory `duckdb.connect(":memory:")` connection reused across all tests.

For each test:
1. `CREATE OR REPLACE TEMP TABLE` with inferred/explicit column types (mapped via `FLINK_TO_DUCKDB_TYPES`)
2. `INSERT INTO ... VALUES` with Python values converted to SQL literals
3. Execute the SQL under test
4. Return rows as `list[dict]`

The type mapping (`_map_type`) handles common Flink -> DuckDB differences: `STRING` -> `VARCHAR`, `TIMESTAMP(3)` -> `TIMESTAMP`, `TIMESTAMP_LTZ(3)` -> `TIMESTAMP WITH TIME ZONE`. Unknown types pass through -- DuckDB is permissive enough to handle most things.

**Important**: `CREATE OR REPLACE TEMP` means tables from a previous test can be overwritten. The connection is deliberately shared so DuckDB stays fast (no reconnect overhead). This does mean tests are not isolated from each other -- a test could in theory SELECT from a table created by a prior test. This is a known trade-off.

### `backends/flink_backend.py` -- PyFlink backend

Maintains two lazy-initialized `TableEnvironment` instances: one for batch, one for streaming. The streaming env has `parallelism.default = 1` for deterministic output.

**Both modes use the same table creation strategy** (`_create_table`):
- Writes input rows to a newline-delimited JSON file in a temp directory
- Issues `DROP TEMPORARY TABLE IF EXISTS` to avoid collisions across tests
- Creates a `TEMPORARY TABLE` with `filesystem` connector pointing to the JSON file
- Optionally adds `PRIMARY KEY (...) NOT ENFORCED` (required for temporal joins)
- Optionally adds `WATERMARK FOR` clause (required for windowed aggregations)
- The filesystem source is bounded in both modes, so queries terminate naturally

This unified approach replaces an earlier design where batch mode used `VALUES` clauses. The filesystem/JSON approach handles complex types (ARRAY, MAP, ROW) that the Flink SQL parser cannot express in `VALUES`.

**Result collection** differs by mode:
- **Batch** (`_collect_results`): Straightforward -- collects all rows using `result.get_resolved_schema().get_column_names()` for column name extraction.
- **Streaming** (`_collect_changelog`): Materializes changelog semantics. Streaming queries emit `+I` (insert), `-U` (update before), `+U` (update after), and `-D` (delete) messages. The collector applies these to a state map keyed by the full row content, keeping only `+I`/`+U` entries and removing `-U`/`-D` entries, yielding the final materialized view.

The `_needs_streaming` function mirrors the regex in `flink_sql_test.py` -- this duplication exists because the backend also needs to decide batch vs streaming independently of the orchestrator's backend selection.

## Points of interest

### 1. The DuckDB dialect gap

DuckDB and Flink SQL are not the same dialect. The current approach is "run it and let it fail" -- there is no SQL transpilation. This works for ~80% of queries (standard SQL is standard SQL) but will break on:
- `STRING` type in DDL (mapped to `VARCHAR`, but could appear in `CAST` expressions in the SQL itself)
- Flink-specific functions (`PROCTIME()`, `CURRENT_WATERMARK()`, `UNIX_TIMESTAMP()` with Flink semantics)
- Array/Map/Row constructors differ (`ARRAY['a', 'b']` in Flink vs `['a', 'b']` in DuckDB)
- `CROSS JOIN UNNEST` syntax varies between engines

A future improvement could add a lightweight SQL rewriter or integrate [SQLGlot](https://github.com/tobymao/sqlglot) for dialect transpilation (this is what SQLMesh does).

### 2. No test isolation in DuckDB

The single `duckdb.connect(":memory:")` connection is shared across all tests within a run. `CREATE OR REPLACE TEMP TABLE` prevents collisions on table names, but if two tests define different schemas for the same table name in different files, the execution order matters. If this becomes a problem, the simplest fix is to use `conn.execute("DROP TABLE IF EXISTS ...")` before each test or create a fresh connection per test (at the cost of ~1ms per test).

### 3. The streaming detection regex is duplicated

`STREAMING_PATTERNS` appears in both `flink_sql_test.py:21` and `flink_backend.py:18`. They serve slightly different purposes (backend selection vs batch/streaming mode within the Flink backend) but the patterns are identical. Consider extracting to a shared constant if they drift.

### 4. Type inference is "first row wins"

`TableInput.infer_schema()` looks at `self.rows[0]` to determine column types. If the first row has `null` for a column, it infers `STRING`. If later rows have integers in that column, the created table column is still `STRING`. Explicit schemas avoid this problem entirely.

### 5. Partial column matching uses `expected[0].keys()`

The set of columns to compare is derived from the first expected row. If different expected rows have different keys, only the keys from row 0 are used. This is intentional (keeps things simple) but worth knowing.

### 6. Two watermark formats are supported

The watermark can be declared in two ways. Both are supported by the parser in `_parse_table_input`:

**Inline** (inside the schema list, legacy format from `test_streaming.yaml`):
```yaml
schema:
  - {name: col1, type: INT}
  - {name: event_time, type: "TIMESTAMP(3)"}
  - watermark: "event_time AS event_time - INTERVAL '5' SECOND"
```

**Top-level** (sibling of schema, preferred format):
```yaml
schema:
  - {name: col1, type: INT}
  - {name: event_time, type: "TIMESTAMP(3)"}
watermark: "event_time AS event_time - INTERVAL '5' SECOND"
```

The parser checks `table_def.get("watermark")` first, then falls back to scanning the schema list. If both are present, the inline one wins (it overwrites the variable last).

### 7. Changelog materialization assumes full-row keys

`_collect_changelog` uses the full row (all column values) as the deduplication key. This works correctly because Flink's `-U` (update before) messages always contain the exact same values as the preceding `+I` or `+U` they retract. However, this means the materialization is sensitive to floating-point representation -- if a retraction message has a slightly different string representation of a float, the `state.pop(key, None)` won't match, leaving stale rows in the result. In practice this hasn't been an issue because the values originate from the same Flink operator.

### 8. PyFlink installation is version-sensitive

`apache-flink` depends on `apache-beam`, which depends on `grpcio-tools`, which uses `pkg_resources` from `setuptools` at build time. `setuptools >= 78` removed `pkg_resources`, so installing apache-flink requires either:
- Pinning `setuptools<78` before installing, and using `pip install --no-build-isolation`
- Using Python 3.11 (apache-flink doesn't officially support 3.12+)

This is documented in the README quick start section.

### 9. Comparison rounding

`_normalize_value` rounds floats to 6 decimal places. This prevents false failures from floating-point arithmetic but means you cannot test precision beyond 6 digits. For financial calculations that need exact decimal comparison, use string assertions or consider adding a configurable tolerance.

## Adding a new backend

1. Create `backends/my_backend.py` implementing `Backend`
2. Add the import to `get_backend()` in `flink_sql_test.py`
3. Add the name to the `--backend` choices in `argparse`
4. If the backend has special SQL requirements, update `detect_backend()` or add a new `backend:` value for YAML tests

## Adding a new test feature

Common extension points:

| Feature | Where to change |
|---|---|
| New YAML field on tests (e.g., `timeout`) | `TestCase` dataclass + `_parse_test()` in `models.py` |
| New comparison mode (e.g., approximate matching) | `compare_results()` in `comparator.py` |
| New table input option (e.g., CSV file reference) | `TableInput` dataclass + `_parse_table_input()` in `models.py`, then handle in each backend |
| New table DDL constraint (e.g., unique key) | `TableInput` dataclass + `_parse_table_input()` in `models.py`, `_create_table()` in `flink_backend.py` (see `primary_key` as an example) |
| New CLI flag | `argparse` block in `flink_sql_test.py:main()` |
| New auto-detection rule | `detect_backend()` in `flink_sql_test.py` and `_needs_streaming()` in `flink_backend.py` |
| New lint rule | Add function with signature `(TestCase) -> list[LintResult]` in `linter.py`, register in `AST_RULES` or `CONTEXT_RULES` list |
