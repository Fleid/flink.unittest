# Changelog

## [0.2.0] - 2026-03-03

### Changed

- **Apache Arrow as the canonical type layer for DuckDB backend.** The DuckDB backend now uses Arrow tables as the intermediate representation between YAML test fixtures and DuckDB. Input tables are converted to Arrow via `pa.Table` and registered with `conn.register()` (zero-copy), replacing the previous approach of generating `CREATE TABLE` DDL and `INSERT ... VALUES` statements with SQL string escaping. Query results are extracted via `fetch_arrow_table().to_pylist()` instead of `fetchall()` with manual column zipping.
- **`pyarrow>=14.0` is now a core dependency** (previously optional under `[parquet]`). This is required because Arrow is now part of the type system, not just for reading Parquet files. It remains listed in the `parquet` and `all` optional groups for backwards compatibility.

### Added

- **`flink_unittest.arrow_types` module** with three main functions:
  - `flink_type_to_arrow()` — parses Flink SQL type strings (including parameterized types like `DECIMAL(10,2)`, `TIMESTAMP(3)`, `TIMESTAMP_LTZ(3)`, and nested types like `ARRAY<T>`, `MAP<K,V>`, `ROW<f1 T1, f2 T2>`) into Arrow data types.
  - `coerce_value()` — converts Python values from YAML/CSV parsing to match Arrow types (e.g., timestamp strings to `datetime`, numeric values to `Decimal`).
  - `table_input_to_arrow()` — builds a `pa.Table` from a `TableInput` model, handling schema inference, type mapping, and value coercion.
- **`ColumnSchema.to_arrow_type()`** convenience method on the model class.
- **Recursive normalization for nested types in the comparator.** `_normalize_value()` now handles `list` and `dict` values, supporting `ARRAY`, `MAP`, and `ROW` result types from Arrow.

### Removed

- `FLINK_TO_DUCKDB_TYPES` mapping dict from `duckdb_backend.py`.
- `_map_type()`, `_sql_literal()`, and `_create_table_sql()` helper functions from `duckdb_backend.py`. These are fully replaced by the Arrow path.

## [0.1.0] - 2025-05-20

Initial release.

- YAML-defined test fixtures with inline or external data (`rows_file` support for CSV, JSON, JSONL, Parquet, Avro).
- DuckDB backend for instant local SQL testing.
- PyFlink backend for streaming SQL constructs (TUMBLE, HOP, temporal joins, watermarks).
- Partial column matching and order-independent comparison.
- External SQL file references via `sql_file`.
- SQLGlot-based SQL linting (`--lint` flag).
- CLI entry point: `flink-unittest`.
