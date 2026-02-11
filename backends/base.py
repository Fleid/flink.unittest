"""Abstract backend interface for Flink SQL test execution."""

from abc import ABC, abstractmethod
from models import TestCase


class Backend(ABC):
    """Base class for test execution backends."""

    @abstractmethod
    def execute_test(self, test: TestCase) -> list[dict]:
        """Execute a test case and return result rows as list of dicts.

        Creates mock tables from test.given, executes test.sql,
        and returns the result rows as a list of dictionaries.
        """

    def cleanup(self):
        """Optional cleanup after all tests are done."""
