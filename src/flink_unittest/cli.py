#!/usr/bin/env python3
"""Flink SQL Test Runner -- unit test your Flink SQL with YAML-defined fixtures.

Usage:
    flink-unittest tests/                     # Run all tests in directory
    flink-unittest tests/test_basic.yaml      # Run a specific test file
    flink-unittest tests/ --backend duckdb     # Force DuckDB backend
    flink-unittest tests/ --backend flink      # Force PyFlink backend
    flink-unittest tests/ --strict             # Enforce strict column projection
    flink-unittest tests/ --lint               # Run SQL lint checks before execution
"""

import argparse
import re
import sys
import time
from pathlib import Path

from flink_unittest.models import load_tests, TestCase
from flink_unittest.comparator import compare_results

# Streaming SQL patterns that require the Flink backend
STREAMING_PATTERNS = re.compile(
    r"\bTUMBLE\s*\(|\bHOP\s*\(|\bSESSION\s*\(|\bMATCH_RECOGNIZE\b"
    r"|\bFOR\s+SYSTEM_TIME\s+AS\s+OF\b|\bCUMULATE\s*\(",
    re.IGNORECASE,
)


def detect_backend(test: TestCase) -> str:
    """Auto-detect which backend a test needs."""
    if STREAMING_PATTERNS.search(test.sql):
        return "flink"
    for table in test.given:
        if table.watermark:
            return "flink"
    return "duckdb"


def get_backend(name: str):
    """Lazy-load and return a backend instance."""
    if name == "duckdb":
        from flink_unittest.backends.duckdb_backend import DuckDBBackend

        return DuckDBBackend()
    elif name == "flink":
        from flink_unittest.backends.flink_backend import FlinkBackend

        return FlinkBackend()
    else:
        raise ValueError(f"Unknown backend: {name}")


# ANSI colors
GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
DIM = "\033[2m"
BOLD = "\033[1m"
RESET = "\033[0m"


def run_test(
    test: TestCase, backend, strict_override: bool = False
) -> tuple[bool, str, float]:
    """Run a single test case. Returns (passed, message, duration_seconds)."""
    start = time.time()
    try:
        actual = backend.execute_test(test)
        passed, message = compare_results(
            actual,
            test.expect.rows,
            ordered=test.expect.ordered,
            strict=test.expect.strict or strict_override,
        )
        duration = time.time() - start
        return passed, message, duration
    except Exception as e:
        duration = time.time() - start
        return False, f"Error: {e}", duration


def main():
    parser = argparse.ArgumentParser(
        description="Flink SQL Test Runner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "paths",
        nargs="+",
        type=Path,
        help="Test files or directories to run",
    )
    parser.add_argument(
        "--backend",
        choices=["duckdb", "flink", "auto"],
        default="auto",
        help="Backend to use (default: auto-detect per test)",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Enforce strict column projection on all tests (no extra columns, exact order)",
    )
    parser.add_argument(
        "--lint",
        action="store_true",
        help="Run SQL lint checks before test execution (requires sqlglot for AST rules)",
    )
    args = parser.parse_args()

    # Set up linter if requested
    lint_enabled = args.lint
    if lint_enabled:
        from flink_unittest.linter import lint_available, lint_test, LintLevel

        if not lint_available():
            print(
                f"{YELLOW}Lint: sqlglot not installed -- only context-based checks will run. "
                f"Install with: pip install sqlglot{RESET}"
            )

    # Load all tests
    all_tests = []
    for path in args.paths:
        try:
            all_tests.extend(load_tests(path))
        except Exception as e:
            print(f"{RED}Error loading {path}: {e}{RESET}")
            sys.exit(1)

    if not all_tests:
        print(f"{YELLOW}No tests found.{RESET}")
        sys.exit(0)

    print(f"\n{BOLD}Flink SQL Test Runner{RESET}")
    print(f"Found {len(all_tests)} test(s)\n")

    # Group tests by backend and lazy-init
    backends = {}
    passed_count = 0
    failed_count = 0
    skipped_count = 0
    lint_warn_count = 0
    lint_error_count = 0
    results = []

    for file_path, test in all_tests:
        # Determine backend
        if test.backend:
            backend_name = test.backend
        elif args.backend != "auto":
            backend_name = args.backend
        else:
            backend_name = detect_backend(test)

        # Run lint checks if enabled (before backend init so skipped tests still get linted)
        if lint_enabled:
            lint_results = lint_test(test)
            for lr in lint_results:
                color = YELLOW if lr.level == LintLevel.WARN else RED
                print(f"  {color}{lr.level.value}{RESET} {test.name}: {lr.message}")
                if lr.level == LintLevel.WARN:
                    lint_warn_count += 1
                else:
                    lint_error_count += 1

        # Get or create backend instance
        if backend_name not in backends:
            try:
                if backend_name == "flink":
                    print(f"{DIM}Initializing PyFlink runtime...{RESET}")
                backends[backend_name] = get_backend(backend_name)
            except ImportError as e:
                print(f"  {YELLOW}SKIP{RESET} {test.name} {DIM}({e}){RESET}")
                skipped_count += 1
                results.append((test.name, "skip", str(e), 0))
                continue

        backend = backends[backend_name]

        # Run the test
        passed, message, duration = run_test(test, backend, strict_override=args.strict)
        ms = duration * 1000

        if passed:
            print(
                f"  {GREEN}PASS{RESET} {test.name} {DIM}[{backend_name}, {ms:.0f}ms]{RESET}"
            )
            passed_count += 1
        else:
            print(
                f"  {RED}FAIL{RESET} {test.name} {DIM}[{backend_name}, {ms:.0f}ms]{RESET}"
            )
            # Indent the failure message
            for line in message.split("\n"):
                print(f"       {line}")
            failed_count += 1

        results.append((test.name, "pass" if passed else "fail", message, duration))

    # Cleanup backends
    for backend in backends.values():
        backend.cleanup()

    # Summary
    print(f"\n{'=' * 50}")
    total = passed_count + failed_count + skipped_count
    parts = []
    if passed_count:
        parts.append(f"{GREEN}{passed_count} passed{RESET}")
    if failed_count:
        parts.append(f"{RED}{failed_count} failed{RESET}")
    if skipped_count:
        parts.append(f"{YELLOW}{skipped_count} skipped{RESET}")
    print(f"{BOLD}{total} tests:{RESET} {', '.join(parts)}")

    if lint_enabled and (lint_warn_count or lint_error_count):
        lint_parts = []
        if lint_warn_count:
            lint_parts.append(f"{lint_warn_count} warning(s)")
        if lint_error_count:
            lint_parts.append(f"{lint_error_count} error(s)")
        print(f"{YELLOW}Lint: {', '.join(lint_parts)}{RESET}")

    sys.exit(1 if failed_count > 0 else 0)


if __name__ == "__main__":
    main()
