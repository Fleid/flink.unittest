"""File readers for external test data (CSV, JSON, Parquet, Avro).

All readers return list[dict] so the rest of the system is format-agnostic.
"""

import csv
import json
from pathlib import Path

from flink_unittest.models import ColumnSchema


# Flink SQL types that map to Python int
_INT_TYPES = {"INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT"}

# Flink SQL types that map to Python float
_FLOAT_TYPES = {"FLOAT", "DOUBLE"}


def _auto_coerce_value(value: str):
    """Auto-coerce a CSV string value to a Python type.

    Tries int -> float -> bool -> None (empty) -> string.
    This matches what YAML does implicitly when parsing inline values.
    """
    if value == "":
        return None
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        pass
    if value.lower() == "true":
        return True
    if value.lower() == "false":
        return False
    return value


def _schema_coerce_value(value: str, flink_type: str):
    """Coerce a CSV string value using a Flink SQL type hint."""
    if value == "":
        return None
    upper = flink_type.upper().strip()
    if upper in _INT_TYPES:
        return int(value)
    if upper in _FLOAT_TYPES or upper.startswith("DECIMAL"):
        return float(value)
    if upper == "BOOLEAN":
        return value.lower() == "true"
    # STRING, VARCHAR, TIMESTAMP, DATE -- leave as string
    return value


def _auto_coerce_row(row: dict) -> dict:
    return {k: _auto_coerce_value(v) for k, v in row.items()}


def _coerce_row_with_schema(row: dict, type_map: dict[str, str]) -> dict:
    return {
        k: _schema_coerce_value(v, type_map[k])
        if k in type_map
        else _auto_coerce_value(v)
        for k, v in row.items()
    }


# ---------------------------------------------------------------------------
# Format-specific readers
# ---------------------------------------------------------------------------


def _read_csv(path: Path, schema: list[ColumnSchema] | None) -> list[dict]:
    with open(path, newline="") as f:
        rows = list(csv.DictReader(f))
    if schema:
        type_map = {col.name: col.type for col in schema}
        return [_coerce_row_with_schema(row, type_map) for row in rows]
    return [_auto_coerce_row(row) for row in rows]


def _read_json(path: Path) -> list[dict]:
    with open(path) as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError(f"JSON file must contain an array of objects: {path}")
    return data


def _read_jsonl(path: Path) -> list[dict]:
    with open(path) as f:
        return [json.loads(line) for line in f if line.strip()]


def _read_parquet(path: Path) -> list[dict]:
    try:
        import pyarrow.parquet as pq
    except ImportError:
        raise ImportError(
            "pyarrow is required to read Parquet files. Install with: pip install pyarrow"
        )
    table = pq.read_table(str(path))
    return table.to_pylist()


def _read_avro(path: Path) -> list[dict]:
    try:
        from fastavro import reader
    except ImportError:
        raise ImportError(
            "fastavro is required to read Avro files. Install with: pip install fastavro"
        )
    with open(path, "rb") as f:
        return list(reader(f))


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

# Extension -> reader function
_READERS = {
    ".csv": lambda p, s: _read_csv(p, s),
    ".json": lambda p, s: _read_json(p),
    ".jsonl": lambda p, s: _read_jsonl(p),
    ".ndjson": lambda p, s: _read_jsonl(p),
    ".parquet": lambda p, s: _read_parquet(p),
    ".avro": lambda p, s: _read_avro(p),
}


def read_rows_file(path: Path, schema: list[ColumnSchema] | None = None) -> list[dict]:
    """Read rows from a data file. Format is inferred from extension.

    Args:
        path: Path to the data file.
        schema: Optional column schema for type coercion (used by CSV reader).

    Returns:
        List of row dicts.
    """
    ext = path.suffix.lower()
    reader_fn = _READERS.get(ext)
    if reader_fn is None:
        supported = ", ".join(sorted(_READERS.keys()))
        raise ValueError(
            f"Unsupported file format '{ext}': {path}\nSupported: {supported}"
        )
    return reader_fn(path, schema)
