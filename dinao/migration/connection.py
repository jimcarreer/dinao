"""Migration connection wrappers providing templated SQL execution."""

from typing import List

from dinao.backend.base import AsyncConnection, Connection, ConnectionPoolBase
from dinao.binding.mappers import DictRowMapper
from dinao.binding.templating import Template


class MigrationConnection:
    """Wraps a sync ``Connection`` with dinao template rendering for migration scripts.

    Provides ``execute`` and ``query`` methods that accept SQL templates
    with ``#{var}`` and ``!{var}`` syntax, rendering them via the pool's
    mung symbol provider before delegating to the underlying connection.
    """

    def __init__(self, cnx: Connection, pool: ConnectionPoolBase):
        """Construct a migration connection.

        :param cnx: the underlying database connection
        :param pool: the connection pool, used for mung symbol access
        """
        self._cnx = cnx
        self._pool = pool
        self._dict_mapper = DictRowMapper()

    @property
    def connection(self) -> Connection:
        """Return the underlying database connection.

        :returns: the wrapped ``Connection``
        """
        return self._cnx

    def execute(self, sql_template: str, **kwargs) -> int:
        """Render and execute a SQL template, returning the affected row count.

        :param sql_template: a dinao SQL template string
        :param kwargs: template parameter values
        :returns: number of rows affected
        """
        template = Template(sql_template)
        sql, params = template.render(self._pool.mung_symbol, kwargs)
        return self._cnx.execute(sql, params, commit=False)

    def query(self, sql_template: str, **kwargs) -> List[dict]:
        """Render and execute a SQL template, returning results as a list of dicts.

        :param sql_template: a dinao SQL template string
        :param kwargs: template parameter values
        :returns: list of dictionaries mapping column names to values
        """
        template = Template(sql_template)
        sql, params = template.render(self._pool.mung_symbol, kwargs)
        with self._cnx.query(sql, params) as results:
            return [self._dict_mapper(row, results.description) for row in results.fetchall()]


class AsyncMigrationConnection:
    """Wraps an ``AsyncConnection`` with dinao template rendering for migration scripts.

    Provides async ``execute`` and ``query`` methods that accept SQL
    templates with ``#{var}`` and ``!{var}`` syntax.
    """

    def __init__(self, cnx: AsyncConnection, pool: ConnectionPoolBase):
        """Construct an async migration connection.

        :param cnx: the underlying async database connection
        :param pool: the connection pool, used for mung symbol access
        """
        self._cnx = cnx
        self._pool = pool
        self._dict_mapper = DictRowMapper()

    @property
    def connection(self) -> AsyncConnection:
        """Return the underlying async database connection.

        :returns: the wrapped ``AsyncConnection``
        """
        return self._cnx

    async def execute(self, sql_template: str, **kwargs) -> int:
        """Render and execute a SQL template, returning the affected row count.

        :param sql_template: a dinao SQL template string
        :param kwargs: template parameter values
        :returns: number of rows affected
        """
        template = Template(sql_template)
        sql, params = template.render(self._pool.mung_symbol, kwargs)
        return await self._cnx.execute(sql, params, commit=False)

    async def query(self, sql_template: str, **kwargs) -> List[dict]:
        """Render and execute a SQL template, returning results as a list of dicts.

        :param sql_template: a dinao SQL template string
        :param kwargs: template parameter values
        :returns: list of dictionaries mapping column names to values
        """
        template = Template(sql_template)
        sql, params = template.render(self._pool.mung_symbol, kwargs)
        async with self._cnx.query(sql, params) as results:
            return [self._dict_mapper(row, results.description) for row in await results.fetchall()]
