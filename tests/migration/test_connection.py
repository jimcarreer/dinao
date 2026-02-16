"""Tests for MigrationConnection and AsyncMigrationConnection."""

from dinao.migration.connection import AsyncMigrationConnection, MigrationConnection

import pytest

from tests.binding.mocks import (
    AsyncMockConnectionPool,
    MockConnectionPool,
    MockDMLCursor,
    MockDQLCursor,
)


class TestMigrationConnection:
    """Tests for the synchronous MigrationConnection wrapper."""

    def test_execute_renders_template(self):
        """Verify execute renders a template and delegates to the underlying connection."""
        pool = MockConnectionPool([MockDMLCursor(affected=1)])
        cnx = pool.lease()
        migration_cnx = MigrationConnection(cnx, pool)
        affected = migration_cnx.execute("INSERT INTO t (col) VALUES (#{val})", val="hello")
        assert affected == 1
        sql, params = cnx.query_stack[0]
        assert "%s" in sql
        assert params == ("hello",)

    def test_execute_with_no_params(self):
        """Verify execute works with plain SQL without template parameters."""
        pool = MockConnectionPool([MockDMLCursor(affected=3)])
        cnx = pool.lease()
        migration_cnx = MigrationConnection(cnx, pool)
        affected = migration_cnx.execute("DELETE FROM t")
        assert affected == 3

    def test_query_returns_dicts(self):
        """Verify query returns results as a list of dictionaries."""
        description = (("id", 0, None, None, None, None, None), ("name", 0, None, None, None, None, None))
        cursor = MockDQLCursor(results=[(1, "alice"), (2, "bob")], description=description)
        pool = MockConnectionPool([cursor])
        cnx = pool.lease()
        migration_cnx = MigrationConnection(cnx, pool)
        rows = migration_cnx.query("SELECT id, name FROM t")
        assert len(rows) == 2
        assert rows[0] == {"id": 1, "name": "alice"}
        assert rows[1] == {"id": 2, "name": "bob"}

    def test_query_with_params(self):
        """Verify query renders template parameters."""
        description = (("id", 0, None, None, None, None, None),)
        cursor = MockDQLCursor(results=[(42,)], description=description)
        pool = MockConnectionPool([cursor])
        cnx = pool.lease()
        migration_cnx = MigrationConnection(cnx, pool)
        rows = migration_cnx.query("SELECT id FROM t WHERE name = #{name}", name="alice")
        assert len(rows) == 1
        assert rows[0] == {"id": 42}

    def test_query_empty_results(self):
        """Verify query returns empty list when no rows match."""
        description = (("id", 0, None, None, None, None, None),)
        cursor = MockDQLCursor(results=[], description=description)
        pool = MockConnectionPool([cursor])
        cnx = pool.lease()
        migration_cnx = MigrationConnection(cnx, pool)
        rows = migration_cnx.query("SELECT id FROM t WHERE 1 = 0")
        assert rows == []

    def test_connection_property(self):
        """Verify the connection property exposes the underlying connection."""
        pool = MockConnectionPool([])
        cnx = pool.lease()
        migration_cnx = MigrationConnection(cnx, pool)
        assert migration_cnx.connection is cnx


class TestAsyncMigrationConnection:
    """Tests for the async AsyncMigrationConnection wrapper."""

    @pytest.mark.asyncio
    async def test_execute_renders_template(self):
        """Verify async execute renders a template and delegates to the underlying connection."""
        pool = AsyncMockConnectionPool([MockDMLCursor(affected=1)])
        cnx = await pool.lease()
        migration_cnx = AsyncMigrationConnection(cnx, pool)
        affected = await migration_cnx.execute("INSERT INTO t (col) VALUES (#{val})", val="hello")
        assert affected == 1

    @pytest.mark.asyncio
    async def test_execute_with_no_params(self):
        """Verify async execute works with plain SQL without template parameters."""
        pool = AsyncMockConnectionPool([MockDMLCursor(affected=5)])
        cnx = await pool.lease()
        migration_cnx = AsyncMigrationConnection(cnx, pool)
        affected = await migration_cnx.execute("DELETE FROM t")
        assert affected == 5

    @pytest.mark.asyncio
    async def test_query_returns_dicts(self):
        """Verify async query returns results as a list of dictionaries."""
        description = (("id", 0, None, None, None, None, None), ("name", 0, None, None, None, None, None))
        cursor = MockDQLCursor(results=[(1, "alice")], description=description)
        pool = AsyncMockConnectionPool([cursor])
        cnx = await pool.lease()
        migration_cnx = AsyncMigrationConnection(cnx, pool)
        rows = await migration_cnx.query("SELECT id, name FROM t")
        assert len(rows) == 1
        assert rows[0] == {"id": 1, "name": "alice"}

    @pytest.mark.asyncio
    async def test_query_with_params(self):
        """Verify async query renders template parameters."""
        description = (("id", 0, None, None, None, None, None),)
        cursor = MockDQLCursor(results=[(99,)], description=description)
        pool = AsyncMockConnectionPool([cursor])
        cnx = await pool.lease()
        migration_cnx = AsyncMigrationConnection(cnx, pool)
        rows = await migration_cnx.query("SELECT id FROM t WHERE name = #{name}", name="bob")
        assert len(rows) == 1
        assert rows[0] == {"id": 99}

    @pytest.mark.asyncio
    async def test_connection_property(self):
        """Verify the connection property exposes the underlying async connection."""
        pool = AsyncMockConnectionPool([])
        cnx = await pool.lease()
        migration_cnx = AsyncMigrationConnection(cnx, pool)
        assert migration_cnx.connection is cnx
