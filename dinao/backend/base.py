"""Defines a basic and primitive database interface."""

import logging
from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import List, Tuple


class ResultSet(ABC):
    """Basic interface definition for result sets (a.k.a rows) returned from Database queries."""

    @abstractmethod
    def fetchone(self) -> tuple:
        """Fetch one result tuple from the underlying cursor.

        If no results are left, None is returned.

        :return: a tuple representing a result row or None
        """
        pass  # pragma: no cover

    @abstractmethod
    def fetchall(self) -> List[tuple]:
        """Fetch the *remaining* result tuples from the underlying cursor.

        If no results are left, an empty list is returned.

        :return: a list of tuples that are the remaining results of the underlying cursor.
        """
        pass  # pragma: no cover

    @abstractmethod
    def columns(self) -> Tuple[str]:
        """Return a tuple representing the column names of the result set.

        :return: a tuple of strings that are column names matching the order of results.
        """
        pass  # pragma: no cover


class Connection(ABC):
    """Basic interface definition for a database connection."""

    def __init__(self, auto_commit: bool = True):
        """Construct a Connection object.

        :param auto_commit: should calls to execute() be automatically committed, defaults to True
        """
        self.logger = logging.getLogger(__name__)
        self._auto_commit = auto_commit

    @abstractmethod
    def commit(self):
        """Commit changes for this connection / transaction to the database."""
        pass  # pragma: no cover

    @abstractmethod
    def rollback(self):
        """Rollback changes for this connection / transaction to the database."""
        pass  # pragma: no cover

    @abstractmethod
    @contextmanager
    def query(self, sql: str, params: tuple = None) -> ResultSet:
        """Execute the given SQL as a statement with the given parameters. Provide the results as context.

        :param sql: the SQL statement(s) to execute
        :param params: the values to bind to the execution of the given SQL
        :returns: a result set representing the query's results
        """
        pass  # pragma: no cover

    @abstractmethod
    def execute(self, sql: str, params: tuple = None, commit: bool = None) -> int:
        """Execute the given SQL as a statement with the given parameters and return the affected row count.

        :param sql: the SQL statement(s) to execute
        :param params: the values to bind to the execution of the given SQL
        :param commit: commit the changes to the database after execution, defaults to value given in constructor
        """
        pass  # pragma: no cover


class ConnectionPool(ABC):
    """Basic interface definition for a pool of database connections."""

    def __init__(self, db_url: str):
        """Construct a connection pool for the given connection URL.

        The db_url is expected to be in the following format::

            "{db_backend}+{driver}://{username}:{password}@{hostname}:{port}/{db_name}?{optional_ars}"

        :param db_url: a url with the described format
        """
        self.logger = logging.getLogger(__name__)
        self._raw_db_url = db_url

    @property
    @abstractmethod
    def mung_symbol(self) -> str:
        """Return the symbol used when replacing variable specifiers in templated SQL."""
        pass  # pragma: no cover

    @abstractmethod
    def lease(self) -> Connection:
        """Lease a connection from the underlying pool."""
        pass  # pragma: no cover

    @abstractmethod
    def release(self, cnx: Connection):
        """Release a connection back to the underlying pool."""
        pass  # pragma: no cover
