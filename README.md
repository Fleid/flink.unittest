# flink-sql-test

Unit test your Flink SQL with YAML-defined fixtures. Inspired by [dbt unit tests](https://docs.getdbt.com/docs/build/unit-tests) and [ksql-test-runner](https://docs.ksqldb.io/en/latest/how-to-guides/test-an-app/).

## Quick start

```bash
pip install pyyaml duckdb
python flink_sql_test.py examples/
```

To also run streaming tests (TUMBLE/HOP windows, temporal joins), install PyFlink in a Python 3.11 virtualenv:

```bash
python3.11 -m venv .venv311
source .venv311/bin/activate
pip install "setuptools<78" pyyaml duckdb
pip install --no-build-isolation apache-flink
python flink_sql_test.py examples/
```

> **Note:** `apache-flink` requires Python 3.11 and `setuptools<78` due to a `pkg_resources` dependency in `apache-beam`. The `--no-build-isolation` flag is needed so the build uses the pinned setuptools.

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
python flink_sql_test.py tests/test_revenue.yaml
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
python flink_sql_test.py tests/ --backend duckdb
```

### PyFlink

Used automatically when the SQL contains streaming constructs: `TUMBLE()`, `HOP()`, `SESSION()`, `MATCH_RECOGNIZE`, `FOR SYSTEM_TIME AS OF`, or when the input table declares a watermark. Requires `apache-flink` to be installed.

```bash
pip install apache-flink
python flink_sql_test.py tests/ --backend flink
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
python flink_sql_test.py [paths...] [--backend duckdb|flink|auto] [--strict]
```

- Pass files or directories. Directories are scanned for `*.yaml` and `*.yml` files.
- Exit code is `0` if all tests pass, `1` if any fail.

```bash
# Run one file
python flink_sql_test.py tests/test_transforms.yaml

# Run all tests in a directory
python flink_sql_test.py tests/

# Run multiple paths
python flink_sql_test.py tests/core/ tests/edge_cases.yaml

# Force a backend
python flink_sql_test.py tests/ --backend duckdb

# Enforce strict column projection on all tests
python flink_sql_test.py tests/ --strict
```

The `--strict` flag applies strict column projection globally -- every test will fail if its actual output has extra columns, missing columns, or columns in the wrong order. This is equivalent to setting `strict: true` on every test's `expect` block. The CLI flag and the per-test YAML setting are OR'd together, so individual tests can still opt in via YAML without the global flag.

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
flink-sql-test/
├── flink_sql_test.py       # CLI entry point
├── models.py               # YAML parsing + data models
├── comparator.py           # Result comparison + diff output
├── backends/
│   ├── base.py             # Abstract backend interface
│   ├── duckdb_backend.py   # DuckDB (instant, pure SQL)
│   └── flink_backend.py    # PyFlink (streaming semantics)
├── requirements.txt
└── examples/
    ├── test_basic.yaml             # Introductory tests
    ├── test_streaming.yaml         # TUMBLE window tests (Flink)
    ├── test_top_patterns.yaml      # Common SQL patterns
    └── test_streaming_patterns.yaml # Streaming-specific patterns
```
