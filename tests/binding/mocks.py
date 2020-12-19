"""Mock implementations of Database interface for use in testing binding and mapping functionality."""

from contextlib import contextmanager
from typing import List, Tuple

from dinao.backend.base import Connection, ConnectionPool, ResultSet


class MockResultSet(ResultSet):
    """Mock implementation of the ResultSet interface for use in testing."""

    def __init__(self, results: List[tuple], affects: int, columns: Tuple[str]):  # noqa: D107
        super().__init__(cursor=None)
        self._results = results
        self._columns = columns
        self._affects = affects

    def fetchone(self) -> tuple:  # noqa: D102
        return self._results.pop(0) if self._results else None

    def fetchall(self) -> List[tuple]:  # noqa: D102
        results = self._results
        self._results = []
        return results

    def columns(self) -> Tuple[str]:  # noqa: D102
        return self._columns


class MockConnection(Connection):
    """Mock implementation of the Connection interface for use in testing."""

    def __init__(self, results_stack: List[MockResultSet]):
        """Construct a mock connection.

        :param results_stack: a list of mock results to return from calls to query() and execute()
        """
        super().__init__(cnx=None)
        self.query_stack = []
        self.released = False
        self.committed = 0
        self.rollbacks = 0
        self.executions = 0
        self.queries = 0
        self.results_stack = results_stack

    def commit(self):  # noqa: D102
        self.committed += 1

    def rollback(self):  # noqa: D102
        self.rollbacks += 1

    @contextmanager
    def query(self, sql: str, params: tuple = None) -> ResultSet:  # noqa: D102
        self.query_stack.append((sql, params))
        self.queries += 1
        if not self.results_stack:  # pragma: no cover
            msg = f"Mocked results exhausted: query(sql={sql}, params={params}), call number {self.queries}"
            raise Exception(msg)
        results: MockResultSet = self.results_stack.pop(0)
        yield results

    def execute(self, sql: str, params: tuple = None, commit: bool = None) -> int:  # noqa: D102
        self.query_stack.append((sql, params))
        self.executions += 1
        if not self.results_stack:  # pragma: no cover
            msg = f"Mocked results exhausted: execute(sql={sql}, params={params}), call number {self.executions}"
            raise Exception(msg)
        results: MockResultSet = self.results_stack.pop(0)
        return results._affects

    def assert_clean(self):  # noqa: D102
        assert self.released, "Connection never released"
        assert self.committed > 0, "Connection did not have commit called"

    def _execute(self, cursor, sql: str, params: tuple = None):
        pass  # pragma: no cover


class MockConnectionPool(ConnectionPool):
    """Mock implementation of the ConnectionPool interface for use in testing."""

    def __init__(self, results_stack: List[MockResultSet]):
        """Construct a mock connection pool.

        :param results_stack: a list of mock results to return from calls to query() and execute() on MockConnections
        """
        super().__init__("mock://user:pass@hostname/dbname?schema=schema")
        self.connection_stack = []
        self.results_stack = results_stack

    @property
    def mung_symbol(self) -> str:  # noqa: D102
        return "%s"

    def lease(self) -> MockConnection:  # noqa: D102
        cnx = MockConnection(self.results_stack)
        self.connection_stack.append(cnx)
        return cnx

    def release(self, cnx: MockConnection):  # noqa: D102
        for known_cnx in self.connection_stack:
            if cnx == known_cnx:
                cnx.released = True
