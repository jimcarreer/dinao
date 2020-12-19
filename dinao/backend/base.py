"""Defines a basic and primitive database interface. It is basically a thin wrapper on DB API 2.0."""

import logging
from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import List, Tuple

from dinao.backend.errors import ConfigurationError


class ResultSet:
    """Basic interface definition for result sets (a.k.a rows) returned from Database queries."""

    def __init__(self, cursor):
        """Construct a result set.

        :param cursor: the underlying DB API 2.0 cursor being wrapped by this object.
        """
        self._cursor = cursor

    def fetchone(self) -> tuple:
        """Fetch one result tuple from the underlying cursor.

        If no results are left, None is returned.

        :return: a tuple representing a result row or None
        """
        return self._cursor.fetchone()

    def fetchall(self) -> List[tuple]:
        """Fetch the *remaining* result tuples from the underlying cursor.

        If no results are left, an empty list is returned.

        :return: a list of tuples that are the remaining results of the underlying cursor.
        """
        return self._cursor.fetchall()

    def columns(self) -> Tuple[str]:
        """Return a tuple representing the column names of the result set.

        :return: a tuple of strings that are column names matching the order of results.
        """
        return tuple([str(d[0]) for d in self._cursor.description])


class Connection(ABC):
    """Basic interface definition for a database connection."""

    def __init__(self, cnx, auto_commit: bool = True):
        """Construct a Connection object.

        :param cnx: the inner DB API 2.0 connection this object wraps
        :param auto_commit: should calls to execute() be automatically committed, defaults to True
        """
        self.logger = logging.getLogger(__name__)
        self._cnx = cnx
        self._auto_commit = auto_commit

    def commit(self):
        """Commit changes for this connection / transaction to the database."""
        self._cnx.commit()

    def rollback(self):
        """Rollback changes for this connection / transaction to the database."""
        self._cnx.rollback()

    @abstractmethod
    def _execute(self, cursor, sql: str, params: tuple = None):
        pass  # pragma: no cover

    @contextmanager
    def query(self, sql: str, params: tuple = None) -> ResultSet:
        """Execute the given SQL as a statement with the given parameters. Provide the results as context.

        :param sql: the SQL statement(s) to execute
        :param params: the values to bind to the execution of the given SQL
        :returns: a result set representing the query's results
        """
        cursor = self._cnx.cursor()
        self._execute(cursor, sql, params)
        yield ResultSet(cursor)
        cursor.close()

    def execute(self, sql: str, params: tuple = None, commit: bool = None) -> int:
        """Execute the given SQL as a statement with the given parameters and return the affected row count.

        :param sql: the SQL statement(s) to execute
        :param params: the values to bind to the execution of the given SQL
        :param commit: commit the changes to the database after execution, defaults to value given in constructor
        """
        commit = commit if commit is not None else self._auto_commit
        cursor = self._cnx.cursor()
        self._execute(cursor, sql, params)
        affected = cursor.rowcount
        if commit:
            self.commit()
        cursor.close()
        return affected


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

    def _parse_additional_arg_int(self, name: str, default: int, additional_args) -> int:
        if name not in additional_args:
            self.logger.debug(f'No "{name}" specified, defaulting to {default}')
            return default
        try:
            assert len(additional_args.get(name)) == 1
            return int(additional_args.get(name)[0])
        except AssertionError:
            raise ConfigurationError(f'Invalid "{name}": only a single value must be specified')
        except ValueError:
            raise ConfigurationError(f'Invalid "{name}": must be integer')

    @staticmethod
    def _raise_for_unexpected_ars(expected_args: List[str], additional_args):
        for name in additional_args.keys():
            if name in expected_args:
                continue
            raise ConfigurationError(f'Unexpected argument "{name}" specified in additional arguments')

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
