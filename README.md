# flink-unittest

Unit test your Flink SQL with YAML-defined fixtures. Inspired by [dbt unit tests](https://docs.getdbt.com/docs/build/unit-tests) and [ksql-test-runner](https://docs.ksqldb.io/en/latest/how-to-guides/test-an-app/).

The tool ships with two backends. **DuckDB** is included by default and provides fast, in-process execution for the most common SQL patterns (filters, joins, aggregations, window functions). **PyFlink** offers full Flink SQL compatibility including streaming operations (TUMBLE/HOP windows, temporal joins, MATCH_RECOGNIZE), but requires a separate install step in a Python 3.11 virtualenv due to `apache-flink`'s dependency constraints.

## Installation

```bash
pip install flink-unittest
```

With that command you only have access to the DuckDB backend (packaged by default). To install the Flink one, follow the steps below.

> **Note:** Because `apache-flink` requires Python 3.11 and `setuptools<78` due to a `pkg_resources` dependency in `apache-beam`, it is not included in the default install.

To also run streaming tests (TUMBLE/HOP windows, temporal joins), and ensure full SQL compatibility, install PyFlink in a Python 3.11 virtualenv:

```bash
python3.11 -m venv .venv311
source .venv311/bin/activate
pip install "setuptools<78" "flink-unittest[all]"
pip install --no-build-isolation apache-flink
flink-unittest examples/
```

If you don't have Python 3.11 installed, you can get it via [pyenv](https://github.com/pyenv/pyenv) (`pyenv install 3.11`) or Homebrew (`brew install python@3.11`).

Other optional dependency groups:

```bash
pip install "flink-unittest[lint]"      # SQL lint checks (sqlglot)
pip install "flink-unittest[parquet]"   # Parquet fixture files (pyarrow)
pip install "flink-unittest[avro]"      # Avro fixture files (fastavro)
pip install "flink-unittest[all]"       # All of the above
```

## Quick start

```bash
pip install flink-unittest
flink-unittest examples/
```


## Writing tests

Each test is a YAML block with three parts: the **SQL** to test, the **given** input tables, and the **expected** output rows.

```yaml
tests:
  - name: test_revenue_by_region
    sql: |
      SELECT
        region,
        SUM(amount) AS total
      FROM orders
      GROUP BY region
    given:
      orders:
        - {region: 'US', amount: 100}
        - {region: 'US', amount: 200}
        - {region: 'EU', amount: 150}
    expect:
      rows:
        - {region: 'EU', total: 150}
        - {region: 'US', total: 300}
```

Save this as `tests/test_revenue.yaml` and run:

```bash
flink-unittest tests/test_revenue.yaml
```

```
Flink SQL Test Runner
Found 1 test(s)

  PASS test_revenue_by_region [duckdb, 2ms]

==================================================
1 tests: 1 passed
```

## Test format reference

### Minimal (shorthand)

The simplest format lists rows directly under each table name. Column types are inferred from the values (int, float, string, boolean).

```yaml
tests:
  - name: my_test
    sql: |
      SELECT id, UPPER(name) AS name_upper FROM users
    given:
      users:
        - {id: 1, name: 'alice'}
        - {id: 2, name: 'bob'}
    expect:
      rows:
        - {id: 1, name_upper: 'ALICE'}
        - {id: 2, name_upper: 'BOB'}
```

### Explicit schema

When you need specific types, watermarks, or complex types like arrays, provide an explicit schema:

```yaml
given:
  events:
    schema:
      - {name: event_id, type: INT}
      - {name: amount, type: "DECIMAL(10,2)"}
      - {name: event_time, type: "TIMESTAMP(3)"}
    watermark: "event_time AS event_time - INTERVAL '5' SECOND"
    rows:
      - {event_id: 1, amount: 100, event_time: '2024-01-01 10:05:00'}
```

For temporal joins, declare a `primary_key` on the versioned table:

```yaml
given:
  currency_rates:
    schema:
      - {name: currency, type: STRING}
      - {name: rate, type: DOUBLE}
      - {name: update_time, type: "TIMESTAMP(3)"}
    primary_key: [currency]
    watermark: "update_time AS update_time - INTERVAL '5' SECOND"
    rows:
      - {currency: 'EUR', rate: 1.10, update_time: '2024-01-01 09:00:00'}
```

Complex types like `ARRAY<ROW<...>>` are supported -- use object format for ROW values in the data:

```yaml
given:
  orders:
    schema:
      - {name: order_id, type: INT}
      - {name: line_items, type: "ARRAY<ROW<item_name STRING, quantity INT>>"}
    rows:
      - {order_id: 1, line_items: [{item_name: "Widget", quantity: 2}, {item_name: "Gadget", quantity: 1}]}
```

### External SQL file

Instead of inlining SQL in the YAML, you can reference an external `.sql` file. The path is resolved relative to the YAML file's directory.

```yaml
tests:
  - name: test_revenue_from_file
    sql_file: sql/revenue_by_region.sql
    given:
      orders:
        - {region: 'US', amount: 100}
        - {region: 'EU', amount: 150}
    expect:
      rows:
        - {region: 'EU', total: 150}
        - {region: 'US', total: 100}
```

`sql` and `sql_file` are mutually exclusive -- specify one or the other.

### External data files

Input data (`given`) and expected output (`expect`) can also be loaded from external files using `rows_file`. The format is inferred from the file extension.

```yaml
tests:
  - name: test_from_files
    sql: |
      SELECT region, SUM(amount) AS total FROM orders GROUP BY region
    given:
      orders:
        rows_file: data/orders.csv
    expect:
      rows_file: data/expected.json
```

Supported formats:

| Extension | Format | Dependency |
|-----------|--------|------------|
| `.csv` | CSV with header row | None (stdlib) |
| `.json` | JSON array of objects | None (stdlib) |
| `.jsonl`, `.ndjson` | Newline-delimited JSON | None (stdlib) |
| `.parquet` | Apache Parquet | `pip install pyarrow` |
| `.avro` | Apache Avro | `pip install fastavro` |

`rows` and `rows_file` are mutually exclusive. Paths are resolved relative to the YAML file's directory.

**CSV type coercion**: CSV values are strings. By default, values are auto-coerced (try int, float, bool, then string). When an explicit `schema` is provided alongside `rows_file`, the schema types guide coercion:

```yaml
given:
  orders:
    schema:
      - {name: id, type: INT}
      - {name: amount, type: DOUBLE}
    rows_file: data/orders.csv
```

### Partial column matching

You don't need to assert every output column. Only the columns listed in `expect.rows` are compared -- extra columns in the actual output are ignored.

```yaml
sql: |
  SELECT id, name, UPPER(name) AS name_upper, id * 10 AS code
  FROM users
expect:
  rows:
    # Only checking name_upper -- id, name, code are ignored
    - {name_upper: 'ALICE'}
    - {name_upper: 'BOB'}
```

### Strict projection

By default, extra columns in the actual output are ignored (partial matching). Set `strict: true` to assert that the actual output has exactly the expected columns, in the same order:

```yaml
sql: |
  SELECT order_id, amount FROM orders
expect:
  strict: true
  rows:
    - {order_id: 1, amount: 100}
```

With `strict: true`, the test fails if:
- The actual output has columns not listed in `expect` (e.g., a forgotten `SELECT *`)
- The actual output is missing columns listed in `expect`
- The column order differs (e.g., `amount, order_id` vs `order_id, amount`)

### Ordered comparison

By default, row order doesn't matter. Set `ordered: true` when order is significant:

```yaml
expect:
  ordered: true
  rows:
    - {product: 'Widget', sales: 5000}
    - {product: 'Gadget', sales: 3000}
    - {product: 'Bolt',   sales: 1000}
```

### Backend override

By default, the runner auto-detects which backend to use per test. You can force a specific backend:

```yaml
tests:
  - name: test_that_needs_flink
    backend: flink
    sql: ...
```

## Backends

### DuckDB (default)

Used automatically for standard SQL (filters, joins, aggregations, window functions, etc.). Zero startup cost, runs in-process.

```bash
flink-unittest tests/ --backend duckdb
```

### PyFlink

Used automatically when the SQL contains streaming constructs: `TUMBLE()`, `HOP()`, `SESSION()`, `MATCH_RECOGNIZE`, `FOR SYSTEM_TIME AS OF`, or when the input table declares a watermark. Requires `apache-flink` to be installed.

```bash
pip install apache-flink
flink-unittest tests/ --backend flink
```

### Auto-detection

The default (`--backend auto`) selects the backend per test:

| SQL construct | Backend |
|---|---|
| `TUMBLE()`, `HOP()`, `SESSION()` | flink |
| `MATCH_RECOGNIZE` | flink |
| `FOR SYSTEM_TIME AS OF` | flink |
| Table with `watermark` defined | flink |
| Everything else | duckdb |

Tests that require PyFlink are gracefully skipped if it's not installed.

## CLI usage

```
flink-unittest [paths...] [--backend duckdb|flink|auto] [--strict] [--lint]
```

- Pass files or directories. Directories are scanned for `*.yaml` and `*.yml` files.
- Exit code is `0` if all tests pass, `1` if any fail.

```bash
# Run one file
flink-unittest tests/test_transforms.yaml

# Run all tests in a directory
flink-unittest tests/

# Run multiple paths
flink-unittest tests/core/ tests/edge_cases.yaml

# Force a backend
flink-unittest tests/ --backend duckdb

# Enforce strict column projection on all tests
flink-unittest tests/ --strict

# Run as a Python module
python -m flink_unittest tests/
```

The `--strict` flag applies strict column projection globally -- every test will fail if its actual output has extra columns, missing columns, or columns in the wrong order. This is equivalent to setting `strict: true` on every test's `expect` block. The CLI flag and the per-test YAML setting are OR'd together, so individual tests can still opt in via YAML without the global flag.

## Linting

The `--lint` flag runs SQL lint checks on each test before execution. Lint warnings and errors are printed inline above the test result.

```bash
pip install "flink-unittest[lint]"
flink-unittest tests/ --lint
```

```
  WARN test_name: SELECT * is fragile -- consider listing columns explicitly
  PASS test_name [duckdb, 2ms]

==================================================
1 tests: 1 passed
Lint: 1 warning(s)
```

### Lint rules

| Rule | Level | Category | What it checks |
|---|---|---|---|
| `select-star` | WARN | AST | `SELECT *` in projection (excluding `COUNT(*)`) |
| `unqualified-column-in-join` | WARN | AST | Columns without table alias in queries with JOINs |
| `group-by-mismatch` | ERROR | AST | Column in SELECT not in GROUP BY and not aggregated |
| `missing-watermark` | WARN | Context | Windowed query (`TUMBLE`/`HOP`/`SESSION`/`CUMULATE`) but input table has no watermark |
| `temporal-join-missing-pk` | WARN | Context | `FOR SYSTEM_TIME AS OF` but no table declares a primary key |

**AST rules** require `sqlglot` to be installed. They parse the SQL into an AST and inspect the tree. If sqlglot is not installed, these rules are silently skipped (a warning is printed once at startup).

**Context rules** use regex matching on the SQL text combined with `TestCase` metadata (watermarks, primary keys). They always run regardless of whether sqlglot is installed.

AST-based rules are automatically skipped for queries containing Flink-specific syntax (`TUMBLE`, `HOP`, `SESSION`, `CUMULATE`, `MATCH_RECOGNIZE`, `FOR SYSTEM_TIME AS OF`) since SQLGlot cannot reliably parse these constructs. Context-based rules still fire for these queries.

## Failure output

When a test fails, you get a clear diff showing exactly what went wrong:

```
  FAIL test_revenue_by_region [duckdb, 2ms]
       1 row(s) differ:

         Row 0:
           total: expected 300, got 250
```

Row count mismatches show both expected and actual tables side by side.

## Supported SQL patterns

These SQL patterns are covered:

| Pattern | Backend | Example |
|---|---|---|
| Filter + transform (CASE, COALESCE, string functions) | duckdb | `test_top_patterns.yaml` |
| GROUP BY aggregation (SUM, COUNT, AVG, MIN, MAX) | duckdb | `test_top_patterns.yaml` |
| JOIN (INNER, LEFT, multi-table) | duckdb | `test_top_patterns.yaml` |
| UNION ALL | duckdb | `test_top_patterns.yaml` |
| DISTINCT | duckdb | `test_top_patterns.yaml` |
| ORDER BY + LIMIT | duckdb | `test_top_patterns.yaml` |
| HAVING | duckdb | `test_top_patterns.yaml` |
| OVER / window functions (ROW_NUMBER, running totals) | duckdb | `test_streaming_patterns.yaml` |
| TUMBLE / HOP windows | flink | `test_streaming.yaml` |
| Temporal joins (`FOR SYSTEM_TIME AS OF`) | flink | `test_streaming_patterns.yaml` |
| CROSS JOIN UNNEST (lateral joins) | flink | `test_top_patterns.yaml` |

## Project structure

```
flink-unittest/
├── pyproject.toml
├── src/flink_unittest/
│   ├── __init__.py             # Package version + get_examples_dir()
│   ├── __main__.py             # python -m flink_unittest support
│   ├── cli.py                  # CLI entry point
│   ├── models.py               # YAML parsing + data models
│   ├── comparator.py           # Result comparison + diff output
│   ├── linter.py               # SQL lint rules (AST + context-based)
│   ├── file_readers.py         # External data file readers (CSV, JSON, Parquet, Avro)
│   ├── backends/
│   │   ├── base.py             # Abstract backend interface
│   │   ├── duckdb_backend.py   # DuckDB (instant, pure SQL)
│   │   └── flink_backend.py    # PyFlink (streaming semantics)
│   └── examples/
│       ├── test_basic.yaml             # Introductory tests
│       ├── test_streaming.yaml         # TUMBLE window tests (Flink)
│       ├── test_top_patterns.yaml      # Common SQL patterns
│       ├── test_streaming_patterns.yaml # Streaming-specific patterns
│       ├── test_sql_file.yaml          # External SQL file reference
│       ├── test_data_files.yaml        # External data file reference (CSV, JSON, JSONL)
│       ├── test_lint_failures.yaml     # Lint rule examples
│       ├── sql/                        # External SQL files
│       │   └── revenue_by_region.sql
│       └── data/                       # External data fixtures
│           ├── orders.csv
│           ├── expected_revenue.json
│           └── users.jsonl
```
