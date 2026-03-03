"""Flink SQL type parsing and Apache Arrow type mapping.

Provides a canonical type layer between Flink SQL type strings and Arrow types,
enabling zero-copy data exchange with DuckDB via conn.register().
"""

import re
from datetime import datetime, date
from decimal import Decimal

import pyarrow as pa

from flink_unittest.models import TableInput


def flink_type_to_arrow(flink_type: str) -> pa.DataType:
    """Parse a Flink SQL type string and return the corresponding Arrow type.

    Handles parameterized types (DECIMAL(p,s), VARCHAR(n), TIMESTAMP(p)),
    and nested types (ARRAY<T>, MAP<K,V>, ROW<f1 T1, f2 T2>) recursively.
    """
    s = flink_type.strip()
    upper = s.upper()

    # Simple scalar types
    simple_map = {
        "STRING": pa.string(),
        "VARCHAR": pa.string(),
        "BOOLEAN": pa.bool_(),
        "TINYINT": pa.int8(),
        "SMALLINT": pa.int16(),
        "INT": pa.int32(),
        "INTEGER": pa.int32(),
        "BIGINT": pa.int64(),
        "FLOAT": pa.float32(),
        "DOUBLE": pa.float64(),
        "DATE": pa.date32(),
        "TIMESTAMP": pa.timestamp("us"),
    }

    if upper in simple_map:
        return simple_map[upper]

    # VARCHAR(n) — ignore length, just string
    if upper.startswith("VARCHAR("):
        return pa.string()

    # DECIMAL(p,s)
    m = re.match(r"DECIMAL\(\s*(\d+)\s*,\s*(\d+)\s*\)", upper)
    if m:
        return pa.decimal128(int(m.group(1)), int(m.group(2)))
    if upper == "DECIMAL":
        return pa.decimal128(38, 18)

    # TIMESTAMP(p) and TIMESTAMP_LTZ(p)
    m = re.match(r"TIMESTAMP_LTZ\(\s*\d+\s*\)", upper)
    if m:
        return pa.timestamp("us", tz="UTC")

    m = re.match(r"TIMESTAMP\(\s*\d+\s*\)", upper)
    if m:
        return pa.timestamp("us")

    # ARRAY<T>
    if upper.startswith("ARRAY<"):
        inner = _extract_angle_bracket_content(s, 5)
        return pa.list_(flink_type_to_arrow(inner))

    # MAP<K,V>
    if upper.startswith("MAP<"):
        inner = _extract_angle_bracket_content(s, 3)
        parts = _split_top_level(inner, ",")
        if len(parts) != 2:
            raise ValueError(f"MAP type expects 2 type args, got {len(parts)}: {s}")
        return pa.map_(flink_type_to_arrow(parts[0]), flink_type_to_arrow(parts[1]))

    # ROW<f1 T1, f2 T2, ...>
    if upper.startswith("ROW<"):
        inner = _extract_angle_bracket_content(s, 3)
        fields = _parse_row_fields(inner)
        return pa.struct(fields)

    raise ValueError(f"Unsupported Flink SQL type: {flink_type}")


def _extract_angle_bracket_content(s: str, prefix_len: int) -> str:
    """Extract content between < and > accounting for nested brackets."""
    # s[prefix_len] should be '<'
    start = prefix_len
    if s[start] != "<":
        raise ValueError(f"Expected '<' at position {start} in: {s}")
    depth = 0
    for i in range(start, len(s)):
        if s[i] == "<":
            depth += 1
        elif s[i] == ">":
            depth -= 1
            if depth == 0:
                return s[start + 1 : i]
    raise ValueError(f"Unbalanced angle brackets in: {s}")


def _split_top_level(s: str, delimiter: str) -> list[str]:
    """Split string by delimiter, but only at the top level (not inside <> or ())."""
    parts = []
    depth = 0
    current = []
    for ch in s:
        if ch in "<(":
            depth += 1
        elif ch in ">)":
            depth -= 1
        if ch == delimiter and depth == 0:
            parts.append("".join(current).strip())
            current = []
        else:
            current.append(ch)
    parts.append("".join(current).strip())
    return parts


def _parse_row_fields(inner: str) -> list[pa.Field]:
    """Parse ROW field definitions like 'f1 STRING, f2 INT' into Arrow fields."""
    parts = _split_top_level(inner, ",")
    fields = []
    for part in parts:
        part = part.strip()
        # Split on first whitespace to get name and type
        tokens = part.split(None, 1)
        if len(tokens) != 2:
            raise ValueError(f"Invalid ROW field definition: {part}")
        name, type_str = tokens
        fields.append(pa.field(name, flink_type_to_arrow(type_str)))
    return fields


def coerce_value(value, arrow_type: pa.DataType):
    """Coerce a Python value to match the target Arrow type.

    Handles common mismatches from YAML/CSV parsing where timestamps arrive
    as strings, decimals as int/float, etc.
    """
    if value is None:
        return None

    # Timestamp: string -> datetime
    if pa.types.is_timestamp(arrow_type):
        if isinstance(value, str):
            return datetime.fromisoformat(value)
        return value

    # Date: string -> date
    if pa.types.is_date(arrow_type):
        if isinstance(value, str):
            return date.fromisoformat(value)
        return value

    # Decimal: int/float -> Decimal
    if pa.types.is_decimal(arrow_type):
        if isinstance(value, (int, float)):
            return Decimal(str(value))
        if isinstance(value, str):
            return Decimal(value)
        return value

    # List/Array: recursively coerce elements
    if pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
        if isinstance(value, list):
            elem_type = arrow_type.value_type
            return [coerce_value(v, elem_type) for v in value]
        return value

    # Map: list of key-value tuples
    if pa.types.is_map(arrow_type):
        if isinstance(value, dict):
            key_type = arrow_type.key_type
            val_type = arrow_type.item_type
            return [
                (coerce_value(k, key_type), coerce_value(v, val_type))
                for k, v in value.items()
            ]
        return value

    # Struct/ROW: recursively coerce dict fields
    if pa.types.is_struct(arrow_type):
        if isinstance(value, dict):
            result = {}
            for i in range(arrow_type.num_fields):
                field = arrow_type.field(i)
                if field.name in value:
                    result[field.name] = coerce_value(value[field.name], field.type)
                else:
                    result[field.name] = None
            return result
        return value

    # Boolean: pass through
    if pa.types.is_boolean(arrow_type):
        return value

    # Integer types: ensure int
    if pa.types.is_integer(arrow_type):
        if isinstance(value, (float, Decimal)):
            return int(value)
        return value

    # Float types: ensure float
    if pa.types.is_floating(arrow_type):
        if isinstance(value, (int, Decimal)):
            return float(value)
        return value

    return value


def table_input_to_arrow(table: TableInput) -> pa.Table:
    """Convert a TableInput to an Arrow table.

    Uses the table's schema (explicit or inferred) to build Arrow types,
    then coerces row values to match before building the table.
    """
    schema_cols = table.infer_schema()
    if not schema_cols:
        # No schema and no rows — return empty table with no columns
        return pa.table({})

    # Build Arrow schema
    arrow_fields = []
    for col in schema_cols:
        arrow_fields.append(pa.field(col.name, flink_type_to_arrow(col.type)))
    arrow_schema = pa.schema(arrow_fields)

    # Build column arrays
    col_names = [col.name for col in schema_cols]
    col_types = [flink_type_to_arrow(col.type) for col in schema_cols]

    columns = {name: [] for name in col_names}
    for row in table.rows:
        for name, atype in zip(col_names, col_types):
            raw_value = row.get(name)
            columns[name].append(coerce_value(raw_value, atype))

    return pa.table(columns, schema=arrow_schema)
