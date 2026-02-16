"""Tests for the asynchronous AsyncMigrationRunner."""

import os
import tempfile

from dinao.backend import create_connection_pool
from dinao.migration.errors import MigrationInProgressError, RevisionError
from dinao.migration.runner.async_ import AsyncMigrationRunner

import pytest


class TestAsyncMigrationRunnerUpgrade:
    """Tests for the async upgrade flow using aiosqlite."""

    @pytest.mark.asyncio
    async def test_upgrade_no_pending_scripts(self, tmp_path):
        """Verify async upgrade does nothing when no scripts match."""
        db_path = str(tmp_path / "test.db")
        scripts_dir = tmp_path / "scripts"
        scripts_dir.mkdir()
        runner = AsyncMigrationRunner(f"sqlite3+aiosqlite://{db_path}", str(scripts_dir))
        await runner.upgrade()

    @pytest.mark.asyncio
    async def test_upgrade_applies_scripts(self, tmp_path):
        """Verify async upgrade applies pending migration scripts."""
        db_path = str(tmp_path / "test.db")
        scripts_dir = tmp_path / "scripts"
        scripts_dir.mkdir()
        (scripts_dir / "20240101_001_create_table.py").write_text(
            'async def upgrade(cnx):\n    await cnx.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")\n'
        )
        (scripts_dir / "20240102_001_insert_data.py").write_text(
            "async def upgrade(cnx):\n" '    await cnx.execute("INSERT INTO t1 (val) VALUES (#{val})", val="hello")\n'
        )
        runner = AsyncMigrationRunner(f"sqlite3+aiosqlite://{db_path}", str(scripts_dir))
        await runner.upgrade()
        # Verify idempotency
        await runner.upgrade()

    @pytest.mark.asyncio
    async def test_upgrade_records_revisions(self, tmp_path):
        """Verify async upgrade records applied revisions in tracking table."""
        db_path = str(tmp_path / "test.db")
        scripts_dir = tmp_path / "scripts"
        scripts_dir.mkdir()
        (scripts_dir / "20240101_001_create_table.py").write_text(
            'async def upgrade(cnx):\n    await cnx.execute("CREATE TABLE t1 (id INTEGER)")\n'
        )
        runner = AsyncMigrationRunner(f"sqlite3+aiosqlite://{db_path}", str(scripts_dir))
        await runner.upgrade()
        pool = create_connection_pool(f"sqlite3+aiosqlite://{db_path}")
        cnx = await pool.lease()
        async with cnx.query("SELECT revision_name, status FROM dinao_migration_revisions") as results:
            rows = await results.fetchall()
        await pool.release(cnx)
        await pool.dispose()
        assert len(rows) == 1
        assert rows[0][0] == "20240101_001_create_table"
        assert rows[0][1] == "success"

    @pytest.mark.asyncio
    async def test_upgrade_script_error_records_failure(self, tmp_path):
        """Verify async upgrade records error state when a script fails."""
        db_path = str(tmp_path / "test.db")
        scripts_dir = tmp_path / "scripts"
        scripts_dir.mkdir()
        (scripts_dir / "20240101_001_failing.py").write_text(
            'async def upgrade(cnx):\n    raise RuntimeError("async failure")\n'
        )
        runner = AsyncMigrationRunner(f"sqlite3+aiosqlite://{db_path}", str(scripts_dir))
        with pytest.raises(RevisionError, match="async failure"):
            await runner.upgrade()
        pool = create_connection_pool(f"sqlite3+aiosqlite://{db_path}")
        cnx = await pool.lease()
        async with cnx.query("SELECT status, error_type FROM dinao_migration_revisions") as results:
            rows = await results.fetchall()
        await pool.release(cnx)
        await pool.dispose()
        assert len(rows) == 1
        assert rows[0][0] == "error"
        assert rows[0][1] == "RuntimeError"

    @pytest.mark.asyncio
    async def test_upgrade_missing_async_upgrade_function(self, tmp_path):
        """Verify ScriptValidationError when script has sync instead of async upgrade."""
        db_path = str(tmp_path / "test.db")
        scripts_dir = tmp_path / "scripts"
        scripts_dir.mkdir()
        (scripts_dir / "20240101_001_bad.py").write_text("def upgrade(cnx):\n    pass\n")
        runner = AsyncMigrationRunner(f"sqlite3+aiosqlite://{db_path}", str(scripts_dir))
        with pytest.raises(RevisionError, match="must define an async"):
            await runner.upgrade()

    @pytest.mark.asyncio
    async def test_upgrade_query_in_script(self, tmp_path):
        """Verify async migration scripts can use the query method."""
        db_path = str(tmp_path / "test.db")
        scripts_dir = tmp_path / "scripts"
        scripts_dir.mkdir()
        (scripts_dir / "20240101_001_create_and_query.py").write_text(
            "async def upgrade(cnx):\n"
            '    await cnx.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")\n'
            '    await cnx.execute("INSERT INTO t1 (val) VALUES (#{val})", val="test")\n'
            '    rows = await cnx.query("SELECT id, val FROM t1")\n'
            "    assert len(rows) == 1\n"
            '    assert rows[0]["val"] == "test"\n'
        )
        runner = AsyncMigrationRunner(f"sqlite3+aiosqlite://{db_path}", str(scripts_dir))
        await runner.upgrade()

    @pytest.mark.asyncio
    async def test_upgrade_multiple_runs_incremental(self, tmp_path):
        """Verify only new scripts are applied on subsequent async runs."""
        db_path = str(tmp_path / "test.db")
        scripts_dir = tmp_path / "scripts"
        scripts_dir.mkdir()
        (scripts_dir / "20240101_001_first.py").write_text(
            'async def upgrade(cnx):\n    await cnx.execute("CREATE TABLE t1 (id INTEGER)")\n'
        )
        runner = AsyncMigrationRunner(f"sqlite3+aiosqlite://{db_path}", str(scripts_dir))
        await runner.upgrade()
        (scripts_dir / "20240102_001_second.py").write_text(
            'async def upgrade(cnx):\n    await cnx.execute("CREATE TABLE t2 (id INTEGER)")\n'
        )
        await runner.upgrade()
        pool = create_connection_pool(f"sqlite3+aiosqlite://{db_path}")
        cnx = await pool.lease()
        async with cnx.query("SELECT revision_name FROM dinao_migration_revisions ORDER BY revision_name") as results:
            rows = await results.fetchall()
        await pool.release(cnx)
        await pool.dispose()
        assert len(rows) == 2
        assert rows[0][0] == "20240101_001_first"
        assert rows[1][0] == "20240102_001_second"

    @pytest.mark.asyncio
    async def test_async_sample_scripts(self):
        """Verify the bundled async sample scripts work end-to-end."""
        sample_dir = os.path.join(os.path.dirname(__file__), "async_scripts")
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")
            runner = AsyncMigrationRunner(f"sqlite3+aiosqlite://{db_path}", sample_dir)
            await runner.upgrade()
            await runner.upgrade()
            # Verify the seed data was inserted by the seed migration
            pool = create_connection_pool(f"sqlite3+aiosqlite://{db_path}")
            cnx = await pool.lease()
            async with cnx.query("SELECT id, name, email FROM users ORDER BY id") as results:
                rows = await results.fetchall()
            await pool.release(cnx)
            await pool.dispose()
            assert len(rows) == 2
            assert rows[0] == (1, "alice", "alice@example.com")
            assert rows[1] == (2, "bob", "bob@example.com")

    @pytest.mark.asyncio
    async def test_upgrade_updates_state(self, tmp_path):
        """Verify async upgrade updates state table on completion."""
        db_path = str(tmp_path / "test.db")
        scripts_dir = tmp_path / "scripts"
        scripts_dir.mkdir()
        (scripts_dir / "20240101_001_create_table.py").write_text(
            'async def upgrade(cnx):\n    await cnx.execute("CREATE TABLE t1 (id INTEGER)")\n'
        )
        runner = AsyncMigrationRunner(f"sqlite3+aiosqlite://{db_path}", str(scripts_dir))
        await runner.upgrade()
        pool = create_connection_pool(f"sqlite3+aiosqlite://{db_path}")
        cnx = await pool.lease()
        async with cnx.query("SELECT status, applied_count FROM dinao_migration_state") as results:
            rows = await results.fetchall()
        await pool.release(cnx)
        await pool.dispose()
        assert len(rows) == 1
        assert rows[0][0] == "success"
        assert rows[0][1] == 1

    @pytest.mark.asyncio
    async def test_upgrade_lock_contention(self, tmp_path):
        """Verify MigrationInProgressError when lock is already held."""
        db_path = str(tmp_path / "test.db")
        scripts_dir = tmp_path / "scripts"
        scripts_dir.mkdir()
        (scripts_dir / "20240101_001_create_table.py").write_text(
            'async def upgrade(cnx):\n    await cnx.execute("CREATE TABLE t1 (id INTEGER)")\n'
        )
        # Pre-create tables and insert a lock row using sync sqlite
        pool = create_connection_pool(f"sqlite3://{db_path}")
        cnx = pool.lease()
        runner = AsyncMigrationRunner(f"sqlite3+aiosqlite://{db_path}", str(scripts_dir))
        cnx.execute(runner.schema_provider.create_revisions_table(), commit=True)
        cnx.execute(runner.schema_provider.create_state_table(), commit=True)
        cnx.execute(
            "INSERT INTO dinao_migration_state (id, status, target_revision) "
            "VALUES (1, 'in_progress', '20240101_001_create_table')",
            commit=True,
        )
        pool.release(cnx)
        pool.dispose()
        with pytest.raises(MigrationInProgressError, match="already in progress"):
            await runner.upgrade()
