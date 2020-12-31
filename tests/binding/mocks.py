"""Mock implementations of Database interface for use in testing binding and mapping functionality."""

from contextlib import contextmanager
from typing import List, Optional, Tuple, Union

from dinao.backend.base import Connection, ConnectionPool, ResultSet


class MockDMLCursor:
    """Mocks a DML (INSERT, UPDATE, ...) DB API 2.0 cursor."""

    def __init__(self, affected: int, fetch_raises: bool = True, describe_raises: bool = True):
        """Construct a mock DML cursor.

        .. note::
            Use fetch_raises and describe_raises to fail tests where DQL like results are not expected.

        :param affected: the row count to return
        :param fetch_raises: flag specifying calls to fetch functions should raise, defaults True
        """
        self._fetch_raises = fetch_raises
        self._describe_raises = describe_raises
        self._rowcount = affected

    def fetchone(self) -> Optional[Tuple]:  # pragma: no cover
        """Mock the fetchone functionality of a DB API 2.0 cursor."""
        if self._fetch_raises:
            raise Exception("A call to fetchone was not expected")
        return None

    def fetchall(self) -> List[Tuple]:  # pragma: no cover
        """Mock the fetchall functionality of a DB API 2.0 cursor."""
        if self._fetch_raises:
            raise Exception("A call to fetchall was not expected")
        return []

    @property
    def rowcount(self) -> int:
        """Mock the row count for a result set."""
        return self._rowcount

    @property
    def description(self) -> Tuple:  # pragma: no cover
        """Mock the description of the result set."""
        if self._describe_raises:
            raise Exception("A call to describe was not expected")
        return tuple()


class MockDQLCursor:
    """Mocks a DQL (basically SELECT queries) DB API 2.0 cursor."""

    def __init__(self, results: List[Tuple], description: Tuple[Tuple, ...]):
        """Construct a mock DQL cursor.

        :param results: the result tuples returned from fetch calls
        :param description: the description of the results returned
        """
        self._rowcount = len(results)
        self._results = results
        self._description = description

    def fetchone(self) -> tuple:
        """Mock the fetchone functionality of a DB API 2.0 cursor."""
        return self._results.pop(0) if self._results else None

    def fetchall(self) -> List[tuple]:  # noqa: D102
        """Mock the fetchall functionality of a DB API 2.0 cursor."""
        results = self._results
        self._results = []
        return results

    @property
    def rowcount(self) -> int:
        """Mock the row count for a result set."""
        return self._rowcount

    @property
    def description(self) -> Tuple:
        """Mock the description of the result set."""
        return self._description


class MockConnection(Connection):
    """Mock implementation of the Connection interface for use in testing."""

    def __init__(self, cursor_stack: List[Union[MockDMLCursor, MockDQLCursor]]):
        """Construct a mock connection.

        :param cursor_stack: a list of mock cursors to return from calls to query() and execute()
        """
        super().__init__(cnx=None)
        self.query_stack = []
        self.released = False
        self.committed = 0
        self.rollbacks = 0
        self.executions = 0
        self.queries = 0
        self.cursor_stack = cursor_stack

    def commit(self):  # noqa: D102
        self.committed += 1

    def rollback(self):  # noqa: D102
        self.rollbacks += 1

    @contextmanager
    def query(self, sql: str, params: tuple = None) -> ResultSet:  # noqa: D102
        self.query_stack.append((sql, params))
        self.queries += 1
        if self.autocommit:
            self.commit()
        if not self.cursor_stack:  # pragma: no cover
            msg = f"Mocked results exhausted: query(sql={sql}, params={params}), call number {self.queries}"
            raise Exception(msg)
        results = ResultSet(cursor=self.cursor_stack.pop(0))
        yield results

    def execute(self, sql: str, params: tuple = None, commit: bool = None) -> int:  # noqa: D102
        self.query_stack.append((sql, params))
        commit = commit if commit is not None else self._auto_commit
        if commit:
            self.commit()
        self.executions += 1
        if not self.cursor_stack:  # pragma: no cover
            msg = f"Mocked results exhausted: execute(sql={sql}, params={params}), call number {self.executions}"
            raise Exception(msg)
        results = ResultSet(cursor=self.cursor_stack.pop(0))
        return results.rowcount

    def assert_clean(self):  # noqa: D102
        assert self.released, "Connection never released"
        assert self.committed > 0, "Connection did not have commit called"

    def _execute(self, cursor, sql: str, params: tuple = None):
        pass  # pragma: no cover


class MockConnectionPool(ConnectionPool):
    """Mock implementation of the ConnectionPool interface for use in testing."""

    def __init__(self, cursor_stack: List[Union[MockDMLCursor, MockDQLCursor]]):
        """Construct a mock connection pool.

        :param cursor_stack: a list of mock results to return from calls to query() and execute() on MockConnections
        """
        super().__init__("mock://user:pass@hostname/dbname?schema=schema")
        self.connection_stack = []
        self.cursor_stack = cursor_stack
        self.disposed = 0

    @property
    def mung_symbol(self) -> str:  # noqa: D102
        return "%s"

    def lease(self) -> MockConnection:  # noqa: D102
        cnx = MockConnection(self.cursor_stack)
        self.connection_stack.append(cnx)
        return cnx

    def release(self, cnx: MockConnection):  # noqa: D102
        for known_cnx in self.connection_stack:
            if cnx == known_cnx:
                cnx.released = True

    def dispose(self):  # noqa: D102
        self.disposed += 1
