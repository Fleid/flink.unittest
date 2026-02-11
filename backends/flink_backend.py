"""PyFlink backend for real Flink SQL semantics."""

import json
import re
import tempfile
from pathlib import Path

from backends.base import Backend
from models import TestCase, TableInput

try:
    from pyflink.table import EnvironmentSettings, TableEnvironment
    from pyflink.common import RowKind
    PYFLINK_AVAILABLE = True
except ImportError:
    PYFLINK_AVAILABLE = False

# Patterns that indicate streaming SQL requiring streaming mode
STREAMING_PATTERNS = [
    r"\bTUMBLE\s*\(",
    r"\bHOP\s*\(",
    r"\bSESSION\s*\(",
    r"\bMATCH_RECOGNIZE\b",
    r"\bFOR\s+SYSTEM_TIME\s+AS\s+OF\b",
    r"\bCUMULATE\s*\(",
]
STREAMING_RE = re.compile("|".join(STREAMING_PATTERNS), re.IGNORECASE)


def _needs_streaming(test: TestCase) -> bool:
    """Check if a test requires streaming mode."""
    if STREAMING_RE.search(test.sql):
        return True
    for table in test.given:
        if table.watermark:
            return True
    return False


def _create_table(t_env, table: TableInput, tmp_dir: Path):
    """Create a table using the filesystem/JSON connector.

    This approach works for both batch and streaming modes and handles
    complex types (ARRAY, MAP, ROW) that VALUES clauses cannot express.
    """
    schema = table.infer_schema()

    # Write rows to a JSON file (newline-delimited JSON)
    json_path = tmp_dir / f"{table.name}.json"
    with open(json_path, "w") as f:
        for row in table.rows:
            json.dump(row, f)
            f.write("\n")

    # Drop existing table to avoid collision across tests
    t_env.execute_sql(f"DROP TEMPORARY TABLE IF EXISTS {table.name}")

    # Build column definitions
    col_defs = []
    for col in schema:
        col_defs.append(f"  `{col.name}` {col.type}")

    # Add primary key if specified (NOT ENFORCED for connector compatibility)
    if table.primary_key:
        pk_cols = ", ".join(f"`{col}`" for col in table.primary_key)
        col_defs.append(f"  PRIMARY KEY ({pk_cols}) NOT ENFORCED")

    # Add watermark if specified
    if table.watermark:
        col_defs.append(f"  WATERMARK FOR {table.watermark}")

    columns_sql = ",\n".join(col_defs)
    escaped_path = str(json_path).replace("\\", "/")

    ddl = (
        f"CREATE TEMPORARY TABLE {table.name} (\n"
        f"{columns_sql}\n"
        f") WITH (\n"
        f"  'connector' = 'filesystem',\n"
        f"  'path' = '{escaped_path}',\n"
        f"  'format' = 'json'\n"
        f")"
    )
    t_env.execute_sql(ddl)


class FlinkBackend(Backend):
    """Execute tests against a local PyFlink TableEnvironment."""

    def __init__(self):
        if not PYFLINK_AVAILABLE:
            raise ImportError(
                "PyFlink is not installed. Install with: pip install apache-flink\n"
                "Or use the DuckDB backend: python flink_sql_test.py --backend duckdb"
            )
        self._batch_env = None
        self._streaming_env = None
        self._tmp_dir = Path(tempfile.mkdtemp(prefix="flink_sql_test_"))

    def _get_batch_env(self) -> "TableEnvironment":
        if self._batch_env is None:
            settings = EnvironmentSettings.in_batch_mode()
            self._batch_env = TableEnvironment.create(settings)
        return self._batch_env

    def _get_streaming_env(self) -> "TableEnvironment":
        if self._streaming_env is None:
            settings = EnvironmentSettings.in_streaming_mode()
            self._streaming_env = TableEnvironment.create(settings)
            # Set parallelism to 1 for deterministic results
            self._streaming_env.get_config().set(
                "parallelism.default", "1"
            )
        return self._streaming_env

    def execute_test(self, test: TestCase) -> list[dict]:
        streaming = _needs_streaming(test)

        if streaming:
            return self._execute_streaming(test)
        else:
            return self._execute_batch(test)

    def _collect_results(self, result) -> list[dict]:
        """Collect results from a TableResult, handling column names."""
        col_names = result.get_resolved_schema().get_column_names()
        rows_out = []
        with result.collect() as results:
            for row in results:
                row_dict = {col_names[i]: row[i] for i in range(len(col_names))}
                rows_out.append(row_dict)
        return rows_out

    def _collect_changelog(self, result) -> list[dict]:
        """Collect streaming results, materializing changelog to final state.

        Streaming queries emit +I (insert), -U (update before), +U (update after),
        -D (delete). We apply these to reconstruct the final materialized view.
        """
        col_names = result.get_resolved_schema().get_column_names()
        state = {}
        with result.collect() as results:
            for row in results:
                kind = row.get_row_kind()
                row_dict = {col_names[i]: row[i] for i in range(len(col_names))}
                key = tuple(sorted((k, str(v) if v is not None else "") for k, v in row_dict.items()))
                if kind in (RowKind.INSERT, RowKind.UPDATE_AFTER):
                    state[key] = row_dict
                elif kind in (RowKind.UPDATE_BEFORE, RowKind.DELETE):
                    state.pop(key, None)
        return list(state.values())

    def _execute_batch(self, test: TestCase) -> list[dict]:
        t_env = self._get_batch_env()

        # Create tables using filesystem connector
        for table in test.given:
            _create_table(t_env, table, self._tmp_dir)

        # Execute the query
        result = t_env.execute_sql(test.sql)
        return self._collect_results(result)

    def _execute_streaming(self, test: TestCase) -> list[dict]:
        t_env = self._get_streaming_env()

        # Create tables using filesystem connector
        for table in test.given:
            _create_table(t_env, table, self._tmp_dir)

        # Execute the query
        result = t_env.execute_sql(test.sql)
        return self._collect_changelog(result)

    def cleanup(self):
        # Clean up temp files
        import shutil
        if self._tmp_dir.exists():
            shutil.rmtree(self._tmp_dir, ignore_errors=True)
