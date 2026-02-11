"""DuckDB backend for fast local SQL testing."""

import duckdb
from backends.base import Backend
from models import TestCase, TableInput


# Flink SQL type -> DuckDB type mapping
FLINK_TO_DUCKDB_TYPES = {
    "STRING": "VARCHAR",
    "INT": "INTEGER",
    "INTEGER": "INTEGER",
    "BIGINT": "BIGINT",
    "SMALLINT": "SMALLINT",
    "TINYINT": "TINYINT",
    "FLOAT": "FLOAT",
    "DOUBLE": "DOUBLE",
    "BOOLEAN": "BOOLEAN",
    "DATE": "DATE",
    "TIMESTAMP": "TIMESTAMP",
    "TIMESTAMP(3)": "TIMESTAMP",
    "TIMESTAMP(6)": "TIMESTAMP",
    "TIMESTAMP_LTZ(3)": "TIMESTAMP WITH TIME ZONE",
}


def _map_type(flink_type: str) -> str:
    """Map a Flink SQL type to a DuckDB type."""
    upper = flink_type.upper().strip()
    if upper in FLINK_TO_DUCKDB_TYPES:
        return FLINK_TO_DUCKDB_TYPES[upper]
    # DECIMAL(p,s) passes through
    if upper.startswith("DECIMAL"):
        return upper
    # VARCHAR(n) passes through
    if upper.startswith("VARCHAR"):
        return upper
    # Default: pass through and let DuckDB handle it
    return flink_type


def _sql_literal(value) -> str:
    """Convert a Python value to a SQL literal string."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        escaped = value.replace("'", "''")
        return f"'{escaped}'"
    return f"'{value}'"


def _create_table_sql(table: TableInput) -> str:
    """Generate CREATE TABLE + INSERT for a mock table in DuckDB."""
    if not table.rows:
        schema = table.infer_schema()
        cols = ", ".join(f"{c.name} {_map_type(c.type)}" for c in schema)
        return f"CREATE OR REPLACE TEMP TABLE {table.name} ({cols});"

    schema = table.infer_schema()
    col_defs = ", ".join(f"{c.name} {_map_type(c.type)}" for c in schema)
    col_names = [c.name for c in schema]

    # Build VALUES rows
    value_rows = []
    for row in table.rows:
        values = ", ".join(_sql_literal(row.get(col)) for col in col_names)
        value_rows.append(f"({values})")

    values_clause = ",\n    ".join(value_rows)
    return (
        f"CREATE OR REPLACE TEMP TABLE {table.name} ({col_defs});\n"
        f"INSERT INTO {table.name} VALUES\n    {values_clause};"
    )


class DuckDBBackend(Backend):
    """Execute tests against an in-memory DuckDB instance."""

    def __init__(self):
        self.conn = duckdb.connect(":memory:")

    def execute_test(self, test: TestCase) -> list[dict]:
        # Create mock tables
        for table in test.given:
            ddl = _create_table_sql(table)
            for statement in ddl.split(";"):
                stmt = statement.strip()
                if stmt:
                    self.conn.execute(stmt)

        # Execute the query under test
        result = self.conn.execute(test.sql)
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()

        return [dict(zip(columns, row)) for row in rows]

    def cleanup(self):
        self.conn.close()
