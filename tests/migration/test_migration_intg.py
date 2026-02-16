"""Integration tests for the migration system against real databases."""

import os
import tempfile

from dinao.migration.runner.async_ import AsyncMigrationRunner
from dinao.migration.runner.sync import MigrationRunner

import pytest


def _write_sync_scripts(scripts_dir):
    """Write sample sync migration scripts to the given directory."""
    os.makedirs(scripts_dir, exist_ok=True)
    with open(os.path.join(scripts_dir, "20240101_001_create_users.py"), "w") as f:
        f.write(
            "def upgrade(cnx):\n"
            "    cnx.execute(\n"
            '        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT)"\n'
            "    )\n"
        )
    with open(os.path.join(scripts_dir, "20240102_001_insert_admin.py"), "w") as f:
        f.write(
            "def upgrade(cnx):\n"
            '    cnx.execute("INSERT INTO users (id, name, email) VALUES (#{id}, #{name}, #{email})",\n'
            '               id=1, name="admin", email="admin@test.com")\n'
            '    rows = cnx.query("SELECT id, name FROM users")\n'
            "    assert len(rows) == 1\n"
            '    assert rows[0]["name"] == "admin"\n'
        )
    with open(os.path.join(scripts_dir, "20240103_001_seed_users.py"), "w") as f:
        f.write(
            "def upgrade(cnx):\n"
            '    cnx.execute("INSERT INTO users (id, name, email) VALUES (#{id}, #{name}, #{email})",\n'
            '               id=2, name="alice", email="alice@test.com")\n'
            '    cnx.execute("INSERT INTO users (id, name, email) VALUES (#{id}, #{name}, #{email})",\n'
            '               id=3, name="bob", email="bob@test.com")\n'
            '    rows = cnx.query("SELECT id, name, email FROM users ORDER BY id")\n'
            "    assert len(rows) == 3\n"
            '    assert rows[0]["name"] == "admin"\n'
            '    assert rows[1]["name"] == "alice"\n'
            '    assert rows[2]["name"] == "bob"\n'
        )


def _write_async_scripts(scripts_dir):
    """Write sample async migration scripts to the given directory."""
    os.makedirs(scripts_dir, exist_ok=True)
    with open(os.path.join(scripts_dir, "20240101_001_create_users.py"), "w") as f:
        f.write(
            "async def upgrade(cnx):\n"
            "    await cnx.execute(\n"
            '        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT)"\n'
            "    )\n"
        )
    with open(os.path.join(scripts_dir, "20240102_001_insert_admin.py"), "w") as f:
        f.write(
            "async def upgrade(cnx):\n"
            '    await cnx.execute("INSERT INTO users (id, name, email) VALUES (#{id}, #{name}, #{email})",\n'
            '                     id=1, name="admin", email="admin@test.com")\n'
            '    rows = await cnx.query("SELECT id, name FROM users")\n'
            "    assert len(rows) == 1\n"
            '    assert rows[0]["name"] == "admin"\n'
        )
    with open(os.path.join(scripts_dir, "20240103_001_seed_users.py"), "w") as f:
        f.write(
            "async def upgrade(cnx):\n"
            '    await cnx.execute("INSERT INTO users (id, name, email) VALUES (#{id}, #{name}, #{email})",\n'
            '                     id=2, name="alice", email="alice@test.com")\n'
            '    await cnx.execute("INSERT INTO users (id, name, email) VALUES (#{id}, #{name}, #{email})",\n'
            '                     id=3, name="bob", email="bob@test.com")\n'
            '    rows = await cnx.query("SELECT id, name, email FROM users ORDER BY id")\n'
            "    assert len(rows) == 3\n"
            '    assert rows[0]["name"] == "admin"\n'
            '    assert rows[1]["name"] == "alice"\n'
            '    assert rows[2]["name"] == "bob"\n'
        )


class TestSQLiteMigrationIntegration:
    """Integration tests for SQLite sync migrations."""

    def test_full_migration_cycle(self):
        """Verify full migration cycle with SQLite."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")
            scripts_dir = os.path.join(tmpdir, "scripts")
            _write_sync_scripts(scripts_dir)
            runner = MigrationRunner(f"sqlite3://{db_path}", scripts_dir)
            runner.upgrade()
            # Second run should be a no-op
            runner.upgrade()


class TestAiosqliteMigrationIntegration:
    """Integration tests for aiosqlite async migrations."""

    @pytest.mark.asyncio
    async def test_full_migration_cycle(self):
        """Verify full async migration cycle with aiosqlite."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")
            scripts_dir = os.path.join(tmpdir, "scripts")
            _write_async_scripts(scripts_dir)
            runner = AsyncMigrationRunner(f"sqlite3+aiosqlite://{db_path}", scripts_dir)
            await runner.upgrade()
            await runner.upgrade()


class TestPostgresMigrationIntegration:
    """Integration tests for PostgreSQL sync migrations."""

    def test_full_migration_cycle(self, tmp_psql_db_url):
        """Verify full migration cycle with PostgreSQL via psycopg2."""
        with tempfile.TemporaryDirectory() as tmpdir:
            scripts_dir = os.path.join(tmpdir, "scripts")
            _write_sync_scripts(scripts_dir)
            runner = MigrationRunner(tmp_psql_db_url, scripts_dir)
            runner.upgrade()
            runner.upgrade()


class TestPsycopg3MigrationIntegration:
    """Integration tests for PostgreSQL sync migrations via psycopg3."""

    def test_full_migration_cycle(self, tmp_psycopg3_db_url):
        """Verify full migration cycle with PostgreSQL via psycopg (v3)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            scripts_dir = os.path.join(tmpdir, "scripts")
            _write_sync_scripts(scripts_dir)
            runner = MigrationRunner(tmp_psycopg3_db_url, scripts_dir)
            runner.upgrade()
            runner.upgrade()


class TestPsycopg3AsyncMigrationIntegration:
    """Integration tests for PostgreSQL async migrations via psycopg3."""

    @pytest.mark.asyncio
    async def test_full_migration_cycle(self, tmp_psycopg3_async_db_url):
        """Verify full async migration cycle with PostgreSQL via psycopg (v3) async."""
        with tempfile.TemporaryDirectory() as tmpdir:
            scripts_dir = os.path.join(tmpdir, "scripts")
            _write_async_scripts(scripts_dir)
            runner = AsyncMigrationRunner(tmp_psycopg3_async_db_url, scripts_dir)
            await runner.upgrade()
            await runner.upgrade()


class TestAsyncpgMigrationIntegration:
    """Integration tests for PostgreSQL async migrations via asyncpg."""

    @pytest.mark.asyncio
    async def test_full_migration_cycle(self, tmp_asyncpg_db_url):
        """Verify full async migration cycle with PostgreSQL via asyncpg."""
        with tempfile.TemporaryDirectory() as tmpdir:
            scripts_dir = os.path.join(tmpdir, "scripts")
            _write_async_scripts(scripts_dir)
            runner = AsyncMigrationRunner(tmp_asyncpg_db_url, scripts_dir)
            await runner.upgrade()
            await runner.upgrade()


class TestMariaDBMigrationIntegration:
    """Integration tests for MariaDB sync migrations."""

    def test_full_migration_cycle(self, tmp_maria_db_url):
        """Verify full migration cycle with MariaDB."""
        with tempfile.TemporaryDirectory() as tmpdir:
            scripts_dir = os.path.join(tmpdir, "scripts")
            _write_sync_scripts(scripts_dir)
            runner = MigrationRunner(tmp_maria_db_url, scripts_dir)
            runner.upgrade()
            runner.upgrade()


class TestMySQLMigrationIntegration:
    """Integration tests for MySQL sync migrations."""

    def test_full_migration_cycle(self, tmp_mysql_db_url):
        """Verify full migration cycle with MySQL."""
        with tempfile.TemporaryDirectory() as tmpdir:
            scripts_dir = os.path.join(tmpdir, "scripts")
            _write_sync_scripts(scripts_dir)
            runner = MigrationRunner(tmp_mysql_db_url, scripts_dir)
            runner.upgrade()
            runner.upgrade()
