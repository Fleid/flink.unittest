"""flink-unittest -- unit test your Flink SQL with YAML-defined fixtures."""

from pathlib import Path

__version__ = "0.1.0"


def get_examples_dir() -> Path:
    """Return the path to the bundled example test files."""
    return Path(__file__).parent / "examples"
