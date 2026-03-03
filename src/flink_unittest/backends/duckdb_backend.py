"""DuckDB backend for fast local SQL testing."""

import duckdb
from flink_unittest.backends.base import Backend
from flink_unittest.models import TestCase
from flink_unittest.arrow_types import table_input_to_arrow


class DuckDBBackend(Backend):
    """Execute tests against an in-memory DuckDB instance."""

    def __init__(self):
        self.conn = duckdb.connect(":memory:")

    def execute_test(self, test: TestCase) -> list[dict]:
        # Register mock tables as Arrow views
        for table in test.given:
            arrow_table = table_input_to_arrow(table)
            self.conn.register(table.name, arrow_table)

        # Execute the query under test
        result = self.conn.execute(test.sql)
        arrow_result = result.fetch_arrow_table()
        rows = arrow_result.to_pylist()

        # Unregister tables to avoid name collisions across tests
        for table in test.given:
            self.conn.unregister(table.name)

        return rows

    def cleanup(self):
        self.conn.close()
