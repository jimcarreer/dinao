"""Defines a basic and primitive database interface. It is basically a thin wrapper on DB API 2.0."""

import logging
from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass
from typing import List, Tuple
from urllib.parse import parse_qs, urlparse

from dinao.backend.errors import ConfigurationError


@dataclass
class ColumnDescriptor:
    """Describes a column in a result set."""

    name: str
    type_code: int
    display_size: int = None
    internal_size: int = None
    precision: int = None
    scale: int = None
    null_ok: bool = None


class ResultSet:
    """Basic interface definition for result sets (a.k.a rows) returned from Database queries."""

    def __init__(self, cursor):
        """Construct a result set.

        :param cursor: the underlying DB API 2.0 cursor being wrapped by this object.
        """
        self._cursor = cursor
        self._columns = None
        self._description = None

    def fetchone(self) -> Tuple:
        """Fetch one result tuple from the underlying cursor.

        If no results are left, None is returned.

        :returns: a tuple representing a result row or None
        """
        return self._cursor.fetchone()

    def fetchall(self) -> List[Tuple]:
        """Fetch the *remaining* result tuples from the underlying cursor.

        If no results are left, an empty list is returned.

        :returns: a list of tuples that are the remaining results of the underlying cursor.
        """
        return self._cursor.fetchall()

    @property
    def description(self) -> Tuple[ColumnDescriptor]:
        """Return a sequence of column descriptions representing the result set.

        :returns: a tuple of ColumnDescriptors
        """
        if not self._description:
            self._description = tuple([ColumnDescriptor(*(d[0:7])) for d in self._cursor.description])
        return self._description

    @property
    def rowcount(self) -> int:
        """Return the row count of the result set.

        :returns: the integer count of the rows in the result set
        """
        return self._cursor.rowcount


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

    @property
    def autocommit(self):
        """Whether commit is called after every call to query(...) and execute(...)."""
        return self._auto_commit

    @autocommit.setter
    def autocommit(self, value: bool):
        self._auto_commit = value

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
        try:
            yield ResultSet(cursor)
        finally:
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

            "{dialect}+{driver}://{username}:{password}@{hostname}:{port}/{db_name}?{optional_args}"

        :param db_url: a url with the described format
        """
        self.logger = logging.getLogger(__name__)
        self._raw_db_url = db_url
        self._db_url = urlparse(self._raw_db_url)
        self._args = parse_qs(self._db_url.query, keep_blank_values=True)

    @staticmethod
    def _strict_bool(value: str):
        if value.lower() not in ["true", "false"]:
            raise ValueError(f"Cannot cast '{value}' to bool")
        return value.lower() == "true"

    def _raise_for_unexpected_args(self):
        unexpected = ",".join(self._args.keys())
        if unexpected:
            raise ConfigurationError(f"Unexpected argument(s): {unexpected}")

    def _get_arg(self, name: str, expected_type, default=None):
        if name not in self._args:
            self.logger.debug(f"No '{name}' specified, defaulting to {default}")
            return default
        caster = expected_type if expected_type is not bool else self._strict_bool
        try:
            if caster != list:
                if len(self._args.get(name)) != 1:
                    raise ConfigurationError(f"Invalid argument '{name}': only a single value must be specified")
                return caster(self._args.pop(name)[0])
            return self._args.pop(name)
        except ValueError as x:
            raise ConfigurationError(f"Invalid argument '{name}': must be {expected_type.__name__}") from x

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

    @abstractmethod
    def dispose(self):
        """Close the pool and clean up any resources it was using."""
        pass  # pragma: no cover
