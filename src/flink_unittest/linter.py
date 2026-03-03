"""SQLGlot-based lint checks for Flink SQL test definitions."""

import logging
import re
from dataclasses import dataclass
from enum import Enum

from flink_unittest.models import TestCase

# SQLGlot is optional -- AST-based rules are skipped if not installed
try:
    import sqlglot
    from sqlglot import exp

    SQLGLOT_AVAILABLE = True
except ImportError:
    SQLGLOT_AVAILABLE = False


class LintLevel(Enum):
    WARN = "WARN"
    ERROR = "ERROR"


@dataclass
class LintResult:
    level: LintLevel
    rule_name: str
    message: str


# Regex patterns for context-based rules and AST reliability check
WINDOW_PATTERN = re.compile(
    r"\bTUMBLE\s*\(|\bHOP\s*\(|\bSESSION\s*\(|\bCUMULATE\s*\(",
    re.IGNORECASE,
)

TEMPORAL_JOIN_PATTERN = re.compile(
    r"\bFOR\s+SYSTEM_TIME\s+AS\s+OF\b",
    re.IGNORECASE,
)

# Flink-specific syntax that SQLGlot cannot reliably parse.
# AST-based rules skip when these are present to avoid false positives.
FLINK_SPECIFIC_PATTERN = re.compile(
    r"\bTUMBLE\s*\(|\bHOP\s*\(|\bSESSION\s*\(|\bCUMULATE\s*\("
    r"|\bFOR\s+SYSTEM_TIME\s+AS\s+OF\b|\bMATCH_RECOGNIZE\b",
    re.IGNORECASE,
)


def lint_available() -> bool:
    """Check if sqlglot is installed for AST-based lint rules."""
    return SQLGLOT_AVAILABLE


def _parse_sql(sql: str):
    """Parse SQL for AST-based lint rules.

    Returns the parsed AST or None if the SQL contains Flink-specific syntax
    that SQLGlot cannot reliably parse (TUMBLE, HOP, temporal joins, etc.).
    """
    # Skip AST analysis for Flink-specific SQL to avoid false positives
    if FLINK_SPECIFIC_PATTERN.search(sql):
        return None
    try:
        # Suppress SQLGlot's WARN-level parse messages for Flink-specific syntax
        logger = logging.getLogger("sqlglot")
        prev_level = logger.level
        logger.setLevel(logging.CRITICAL)
        try:
            parsed = sqlglot.parse(sql, error_level=sqlglot.ErrorLevel.WARN)
        finally:
            logger.setLevel(prev_level)
        if parsed:
            return parsed[0]
        return None
    except Exception:
        return None


# ---------------------------------------------------------------------------
# AST-based rules (require sqlglot)
# ---------------------------------------------------------------------------


def check_select_star(test: TestCase) -> list[LintResult]:
    """Detect SELECT * which is fragile to schema changes."""
    ast = _parse_sql(test.sql)
    if ast is None:
        return []

    for star in ast.find_all(exp.Star):
        # COUNT(*) is fine -- only flag SELECT * at the projection level
        parent = star.parent
        if isinstance(parent, exp.AggFunc):
            continue
        if isinstance(parent, exp.Column) and isinstance(parent.parent, exp.AggFunc):
            continue
        return [LintResult(
            level=LintLevel.WARN,
            rule_name="select-star",
            message="SELECT * is fragile -- consider listing columns explicitly",
        )]
    return []


def check_unqualified_column_in_join(test: TestCase) -> list[LintResult]:
    """Detect unqualified columns in queries with JOINs."""
    ast = _parse_sql(test.sql)
    if ast is None:
        return []

    joins = list(ast.find_all(exp.Join))
    if not joins:
        return []

    unqualified = set()
    for col in ast.find_all(exp.Column):
        if not col.table:
            unqualified.add(col.name)

    if unqualified:
        col_list = ", ".join(sorted(unqualified)[:5])
        suffix = f" (and {len(unqualified) - 5} more)" if len(unqualified) > 5 else ""
        return [LintResult(
            level=LintLevel.WARN,
            rule_name="unqualified-column-in-join",
            message=f"Unqualified column(s) in JOIN query: {col_list}{suffix} -- qualify with table alias to avoid ambiguity",
        )]
    return []


def check_group_by_mismatch(test: TestCase) -> list[LintResult]:
    """Detect columns in SELECT that aren't in GROUP BY and aren't aggregated."""
    ast = _parse_sql(test.sql)
    if ast is None:
        return []

    group = ast.find(exp.Group)
    if group is None:
        return []

    # Collect GROUP BY keys as normalized strings and column names
    group_by_keys = set()
    for expr in group.expressions:
        group_by_keys.add(expr.sql().lower())
        if isinstance(expr, exp.Column):
            group_by_keys.add(expr.name.lower())

    select = ast.find(exp.Select)
    if select is None:
        return []

    results = []
    for projection in select.expressions:
        expr = projection.this if isinstance(projection, exp.Alias) else projection

        if isinstance(expr, exp.AggFunc):
            continue
        if isinstance(expr, exp.Star):
            continue

        if isinstance(expr, exp.Column):
            col_sql = expr.sql().lower()
            col_name = expr.name.lower()
            if col_sql not in group_by_keys and col_name not in group_by_keys:
                results.append(LintResult(
                    level=LintLevel.ERROR,
                    rule_name="group-by-mismatch",
                    message=f"Column '{expr.sql()}' in SELECT is not in GROUP BY and not aggregated",
                ))
    return results


# ---------------------------------------------------------------------------
# Context-based rules (no sqlglot needed, use regex + TestCase metadata)
# ---------------------------------------------------------------------------


def check_missing_watermark(test: TestCase) -> list[LintResult]:
    """Warn when windowed query uses tables without watermarks."""
    if not WINDOW_PATTERN.search(test.sql):
        return []

    tables_without_watermark = [t.name for t in test.given if not t.watermark]
    if tables_without_watermark:
        table_list = ", ".join(tables_without_watermark)
        return [LintResult(
            level=LintLevel.WARN,
            rule_name="missing-watermark",
            message=f"Windowed query but table(s) have no watermark: {table_list}",
        )]
    return []


def check_temporal_join_missing_pk(test: TestCase) -> list[LintResult]:
    """Warn when temporal join is used but no table has a primary key."""
    if not TEMPORAL_JOIN_PATTERN.search(test.sql):
        return []

    has_any_pk = any(t.primary_key for t in test.given)
    if has_any_pk:
        return []

    return [LintResult(
        level=LintLevel.WARN,
        rule_name="temporal-join-missing-pk",
        message="Temporal join (FOR SYSTEM_TIME AS OF) but no table declares a primary_key",
    )]


# ---------------------------------------------------------------------------
# Rule registry and entry point
# ---------------------------------------------------------------------------

AST_RULES = [
    check_select_star,
    check_unqualified_column_in_join,
    check_group_by_mismatch,
]

CONTEXT_RULES = [
    check_missing_watermark,
    check_temporal_join_missing_pk,
]


def lint_test(test: TestCase) -> list[LintResult]:
    """Run all lint rules against a test case.

    AST-based rules require sqlglot and are skipped if not installed.
    Context-based rules use regex + TestCase metadata and always run.
    """
    results = []

    if SQLGLOT_AVAILABLE:
        for rule in AST_RULES:
            results.extend(rule(test))

    for rule in CONTEXT_RULES:
        results.extend(rule(test))

    return results
