"""Result comparison with partial column matching and diff output."""

from decimal import Decimal
from datetime import datetime, date


def _normalize_value(value):
    """Normalize a value for comparison (handle type coercion)."""
    if value is None:
        return None
    # Normalize Decimal to float for comparison
    if isinstance(value, Decimal):
        return float(value)
    # Normalize datetime objects to string for comparison
    if isinstance(value, datetime):
        return str(value)
    if isinstance(value, date):
        return str(value)
    # Normalize numeric types
    if isinstance(value, float):
        # Round to avoid floating point precision issues
        return round(value, 6)
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        return value.strip()
    return value


def _normalize_row(row: dict, columns: list[str] | None = None) -> dict:
    """Normalize a row for comparison. If columns specified, only include those."""
    if columns:
        return {k: _normalize_value(row.get(k)) for k in columns}
    return {k: _normalize_value(v) for k, v in row.items()}


def _row_sort_key(row: dict) -> tuple:
    """Create a sort key from a row dict for order-independent comparison."""
    return tuple(
        (k, str(v) if v is not None else "")
        for k, v in sorted(row.items())
    )


def compare_results(
    actual: list[dict],
    expected: list[dict],
    ordered: bool = False,
    strict: bool = False,
) -> tuple[bool, str]:
    """Compare actual results to expected results.

    Returns (passed, message) where message contains diff details on failure.

    If strict=True, the actual output must have exactly the same columns as the
    expected rows (no extra, no missing) and columns must appear in the same order.
    """
    if not expected:
        if not actual:
            return True, "OK (both empty)"
        return False, f"Expected 0 rows, got {len(actual)}"

    # Determine which columns to compare
    compare_columns = list(expected[0].keys())

    if strict and actual:
        actual_columns = list(actual[0].keys())
        extra = [c for c in actual_columns if c not in compare_columns]
        missing = [c for c in compare_columns if c not in actual_columns]
        if extra or missing:
            parts = []
            if extra:
                parts.append(f"unexpected columns: {extra}")
            if missing:
                parts.append(f"missing columns: {missing}")
            return False, f"Strict column mismatch: {'; '.join(parts)}"
        if actual_columns != compare_columns:
            return False, (
                f"Column order mismatch:\n"
                f"  expected: {compare_columns}\n"
                f"  actual:   {actual_columns}"
            )

    # Normalize rows (strict mode keeps all columns, partial mode projects)
    if strict:
        actual_normalized = [_normalize_row(row) for row in actual]
    else:
        actual_normalized = [_normalize_row(row, compare_columns) for row in actual]
    expected_normalized = [_normalize_row(row) for row in expected]

    if not ordered:
        actual_normalized = sorted(actual_normalized, key=_row_sort_key)
        expected_normalized = sorted(expected_normalized, key=_row_sort_key)

    # Compare row counts first
    if len(actual_normalized) != len(expected_normalized):
        msg = _format_diff(actual_normalized, expected_normalized, compare_columns)
        return False, f"Row count mismatch: expected {len(expected_normalized)}, got {len(actual_normalized)}\n{msg}"

    # Compare row by row
    mismatches = []
    for i, (act, exp) in enumerate(zip(actual_normalized, expected_normalized)):
        if act != exp:
            mismatches.append((i, exp, act))

    if mismatches:
        msg = _format_mismatches(mismatches, compare_columns)
        return False, msg

    return True, "OK"


def _format_diff(actual: list[dict], expected: list[dict], columns: list[str]) -> str:
    """Format a diff between actual and expected results."""
    lines = []

    # Header
    col_widths = {col: max(len(col), 10) for col in columns}
    header = " | ".join(col.ljust(col_widths[col]) for col in columns)
    separator = "-+-".join("-" * col_widths[col] for col in columns)

    lines.append(f"\n  Expected ({len(expected)} rows):")
    lines.append(f"  {header}")
    lines.append(f"  {separator}")
    for row in expected:
        vals = " | ".join(
            str(row.get(col, "")).ljust(col_widths[col]) for col in columns
        )
        lines.append(f"  {vals}")

    lines.append(f"\n  Actual ({len(actual)} rows):")
    lines.append(f"  {header}")
    lines.append(f"  {separator}")
    for row in actual:
        vals = " | ".join(
            str(row.get(col, "")).ljust(col_widths[col]) for col in columns
        )
        lines.append(f"  {vals}")

    return "\n".join(lines)


def _format_mismatches(mismatches: list, columns: list[str]) -> str:
    """Format row-level mismatches."""
    lines = [f"{len(mismatches)} row(s) differ:"]

    for row_idx, expected, actual in mismatches:
        lines.append(f"\n  Row {row_idx}:")
        for col in columns:
            exp_val = expected.get(col)
            act_val = actual.get(col)
            if exp_val != act_val:
                lines.append(f"    {col}: expected {exp_val!r}, got {act_val!r}")

    return "\n".join(lines)
