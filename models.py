"""Data models and YAML parsing for Flink SQL test definitions."""

from dataclasses import dataclass, field
from pathlib import Path
from datetime import datetime, date
from decimal import Decimal
import yaml


@dataclass
class ColumnSchema:
    name: str
    type: str


@dataclass
class TableInput:
    """A mock input table with optional explicit schema and watermark."""
    name: str
    rows: list[dict]
    schema: list[ColumnSchema] | None = None
    watermark: str | None = None  # e.g. "event_time AS event_time - INTERVAL '1' SECOND"
    primary_key: list[str] | None = None  # e.g. ["currency"] for temporal join tables

    def infer_schema(self) -> list[ColumnSchema]:
        """Infer column types from the first row of data."""
        if self.schema:
            return self.schema
        if not self.rows:
            return []

        first_row = self.rows[0]
        columns = []
        for col_name, value in first_row.items():
            columns.append(ColumnSchema(name=col_name, type=_infer_type(value)))
        return columns


@dataclass
class ExpectedOutput:
    """Expected output rows, with optional ordering."""
    rows: list[dict]
    ordered: bool = False  # If True, row order matters
    strict: bool = False   # If True, actual must have exactly these columns in this order


@dataclass
class TestCase:
    """A single test case: SQL + mock inputs + expected output."""
    name: str
    sql: str
    given: list[TableInput]
    expect: ExpectedOutput
    backend: str | None = None  # None = auto-detect


def _infer_type(value) -> str:
    """Infer a Flink SQL type string from a Python value."""
    if isinstance(value, bool):
        return "BOOLEAN"
    if isinstance(value, int):
        return "INT"
    if isinstance(value, float):
        return "DOUBLE"
    if isinstance(value, Decimal):
        return "DECIMAL(10,2)"
    if isinstance(value, datetime):
        return "TIMESTAMP(3)"
    if isinstance(value, date):
        return "DATE"
    return "STRING"


def _parse_table_input(name: str, table_def: dict) -> TableInput:
    """Parse a single table input definition from YAML."""
    rows = table_def.get("rows", [])

    schema = None
    watermark = table_def.get("watermark")
    primary_key = table_def.get("primary_key")
    raw_schema = table_def.get("schema")
    if raw_schema:
        columns = []
        for item in raw_schema:
            if isinstance(item, dict):
                if "watermark" in item:
                    watermark = item["watermark"]
                elif "name" in item and "type" in item:
                    columns.append(ColumnSchema(name=item["name"], type=item["type"]))
        schema = columns if columns else None

    return TableInput(name=name, rows=rows, schema=schema, watermark=watermark, primary_key=primary_key)


def _parse_test(test_def: dict, base_dir: Path) -> TestCase:
    """Parse a single test case from YAML."""
    name = test_def["name"]

    # SQL: inline or from file (mutually exclusive)
    has_sql = "sql" in test_def
    has_sql_file = "sql_file" in test_def
    if has_sql and has_sql_file:
        raise ValueError(f"Test '{name}': specify 'sql' or 'sql_file', not both")
    if not has_sql and not has_sql_file:
        raise ValueError(f"Test '{name}': must specify 'sql' or 'sql_file'")

    if has_sql_file:
        sql_path = (base_dir / test_def["sql_file"]).resolve()
        if not sql_path.is_file():
            raise FileNotFoundError(f"Test '{name}': sql_file not found: {sql_path}")
        sql = sql_path.read_text().strip()
    else:
        sql = test_def["sql"].strip()

    backend = test_def.get("backend")

    # Parse given tables
    given = []
    for table_name, table_def in test_def.get("given", {}).items():
        if isinstance(table_def, list):
            # Shorthand: given.orders: [{...}, {...}]
            given.append(TableInput(name=table_name, rows=table_def))
        elif isinstance(table_def, dict):
            given.append(_parse_table_input(table_name, table_def))

    # Parse expected output
    expect_def = test_def.get("expect", {})
    expect = ExpectedOutput(
        rows=expect_def.get("rows", []),
        ordered=expect_def.get("ordered", False),
        strict=expect_def.get("strict", False),
    )

    return TestCase(name=name, sql=sql, given=given, expect=expect, backend=backend)


def load_test_file(path: Path) -> list[TestCase]:
    """Load all test cases from a YAML file."""
    with open(path) as f:
        data = yaml.safe_load(f)

    if not data or "tests" not in data:
        raise ValueError(f"Invalid test file {path}: missing 'tests' key")

    base_dir = path.parent
    return [_parse_test(t, base_dir) for t in data["tests"]]


def load_tests(path: Path) -> list[tuple[Path, TestCase]]:
    """Load tests from a file or directory. Returns (file_path, test_case) tuples."""
    results = []
    if path.is_file():
        for tc in load_test_file(path):
            results.append((path, tc))
    elif path.is_dir():
        for yaml_file in sorted(path.glob("*.yaml")):
            for tc in load_test_file(yaml_file):
                results.append((yaml_file, tc))
        for yml_file in sorted(path.glob("*.yml")):
            for tc in load_test_file(yml_file):
                results.append((yml_file, tc))
    else:
        raise FileNotFoundError(f"Path not found: {path}")
    return results
