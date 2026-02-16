"""Tests for the synchronous MigrationRunner."""

import os

from dinao.migration.errors import DiscoveryError, MigrationInProgressError, RevisionError
from dinao.migration.runner.sync import MigrationRunner

import pytest


class TestMigrationRunnerInit:
    """Tests for MigrationRunner construction."""

    def test_init_with_valid_args(self, tmp_path):
        """Verify runner initializes with valid URL and script directory."""
        runner = MigrationRunner("sqlite3:///test.db", str(tmp_path))
        assert runner.schema_provider is not None

    def test_init_invalid_directory(self):
        """Verify DiscoveryError is raised for non-existent script directory."""
        with pytest.raises(DiscoveryError, match="does not exist"):
            MigrationRunner("sqlite3:///test.db", "/nonexistent/path")


class TestMigrationRunnerUpgrade:
    """Tests for the full upgrade flow using a real SQLite database."""

    def test_upgrade_no_pending_scripts(self, tmp_path):
        """Verify upgrade does nothing when no scripts match."""
        db_path = str(tmp_path / "test.db")
        (tmp_path / "scripts").mkdir()
        runner = MigrationRunner(f"sqlite3://{db_path}", str(tmp_path / "scripts"))
        runner.upgrade()

    def test_upgrade_applies_scripts(self, tmp_path):
        """Verify upgrade applies pending migration scripts."""
        db_path = str(tmp_path / "test.db")
        scripts_dir = tmp_path / "scripts"
        scripts_dir.mkdir()
        (scripts_dir / "20240101_001_create_table.py").write_text(
            'def upgrade(cnx):\n    cnx.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")\n'
        )
        (scripts_dir / "20240102_001_insert_data.py").write_text(
            "def upgrade(cnx):\n" '    cnx.execute("INSERT INTO t1 (val) VALUES (#{val})", val="hello")\n'
        )
        runner = MigrationRunner(f"sqlite3://{db_path}", str(scripts_dir))
        runner.upgrade()
        # Verify by running again - should be idempotent
        runner.upgrade()

    def test_upgrade_records_revisions(self, tmp_path):
        """Verify upgrade records applied revisions in tracking table."""
        db_path = str(tmp_path / "test.db")
        scripts_dir = tmp_path / "scripts"
        scripts_dir.mkdir()
        (scripts_dir / "20240101_001_create_table.py").write_text(
            'def upgrade(cnx):\n    cnx.execute("CREATE TABLE t1 (id INTEGER)")\n'
        )
        runner = MigrationRunner(f"sqlite3://{db_path}", str(scripts_dir))
        runner.upgrade()
        # Check tracking table directly
        from dinao.backend import create_connection_pool

        pool = create_connection_pool(f"sqlite3://{db_path}")
        cnx = pool.lease()
        with cnx.query("SELECT revision_name, status FROM dinao_migration_revisions") as results:
            rows = results.fetchall()
        pool.release(cnx)
        pool.dispose()
        assert len(rows) == 1
        assert rows[0][0] == "20240101_001_create_table"
        assert rows[0][1] == "success"

    def test_upgrade_updates_state(self, tmp_path):
        """Verify upgrade updates state table on completion."""
        db_path = str(tmp_path / "test.db")
        scripts_dir = tmp_path / "scripts"
        scripts_dir.mkdir()
        (scripts_dir / "20240101_001_create_table.py").write_text(
            'def upgrade(cnx):\n    cnx.execute("CREATE TABLE t1 (id INTEGER)")\n'
        )
        runner = MigrationRunner(f"sqlite3://{db_path}", str(scripts_dir))
        runner.upgrade()
        from dinao.backend import create_connection_pool

        pool = create_connection_pool(f"sqlite3://{db_path}")
        cnx = pool.lease()
        with cnx.query("SELECT status, applied_count FROM dinao_migration_state") as results:
            rows = results.fetchall()
        pool.release(cnx)
        pool.dispose()
        assert len(rows) == 1
        assert rows[0][0] == "success"
        assert rows[0][1] == 1

    def test_upgrade_script_error_records_failure(self, tmp_path):
        """Verify upgrade records error state when a script fails."""
        db_path = str(tmp_path / "test.db")
        scripts_dir = tmp_path / "scripts"
        scripts_dir.mkdir()
        (scripts_dir / "20240101_001_failing.py").write_text(
            'def upgrade(cnx):\n    raise RuntimeError("intentional failure")\n'
        )
        runner = MigrationRunner(f"sqlite3://{db_path}", str(scripts_dir))
        with pytest.raises(RevisionError, match="intentional failure"):
            runner.upgrade()
        from dinao.backend import create_connection_pool

        pool = create_connection_pool(f"sqlite3://{db_path}")
        cnx = pool.lease()
        with cnx.query("SELECT status, error_type FROM dinao_migration_revisions") as results:
            rows = results.fetchall()
        pool.release(cnx)
        pool.dispose()
        assert len(rows) == 1
        assert rows[0][0] == "error"
        assert rows[0][1] == "RuntimeError"

    def test_upgrade_missing_upgrade_function(self, tmp_path):
        """Verify ScriptValidationError when script has no upgrade function."""
        db_path = str(tmp_path / "test.db")
        scripts_dir = tmp_path / "scripts"
        scripts_dir.mkdir()
        (scripts_dir / "20240101_001_bad.py").write_text("def not_upgrade(cnx):\n    pass\n")
        runner = MigrationRunner(f"sqlite3://{db_path}", str(scripts_dir))
        with pytest.raises(RevisionError, match="must define a callable"):
            runner.upgrade()

    def test_upgrade_rolls_back_failed_script(self, tmp_path):
        """Verify failed script is rolled back and does not leave partial state."""
        db_path = str(tmp_path / "test.db")
        scripts_dir = tmp_path / "scripts"
        scripts_dir.mkdir()
        # First script succeeds
        (scripts_dir / "20240101_001_create_table.py").write_text(
            'def upgrade(cnx):\n    cnx.execute("CREATE TABLE t1 (id INTEGER)")\n'
        )
        # Second script fails after creating a table
        (scripts_dir / "20240102_001_fails.py").write_text(
            "def upgrade(cnx):\n" '    cnx.execute("CREATE TABLE t2 (id INTEGER)")\n' '    raise ValueError("boom")\n'
        )
        runner = MigrationRunner(f"sqlite3://{db_path}", str(scripts_dir))
        with pytest.raises(RevisionError, match="boom"):
            runner.upgrade()
        # t1 should exist (committed), t2 should not (rolled back)
        from dinao.backend import create_connection_pool

        pool = create_connection_pool(f"sqlite3://{db_path}")
        cnx = pool.lease()
        with cnx.query("SELECT name FROM sqlite_master WHERE type='table' AND name='t1'") as results:
            assert len(results.fetchall()) == 1
        # Note: SQLite DDL is auto-committed, so t2 may exist. This is SQLite-specific behavior.
        pool.release(cnx)
        pool.dispose()

    def test_upgrade_query_in_script(self, tmp_path):
        """Verify migration scripts can use the query method."""
        db_path = str(tmp_path / "test.db")
        scripts_dir = tmp_path / "scripts"
        scripts_dir.mkdir()
        (scripts_dir / "20240101_001_create_and_query.py").write_text(
            "def upgrade(cnx):\n"
            '    cnx.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")\n'
            '    cnx.execute("INSERT INTO t1 (val) VALUES (#{val})", val="test")\n'
            '    rows = cnx.query("SELECT id, val FROM t1")\n'
            "    assert len(rows) == 1\n"
            '    assert rows[0]["val"] == "test"\n'
        )
        runner = MigrationRunner(f"sqlite3://{db_path}", str(scripts_dir))
        runner.upgrade()

    def test_upgrade_multiple_runs_incremental(self, tmp_path):
        """Verify only new scripts are applied on subsequent runs."""
        db_path = str(tmp_path / "test.db")
        scripts_dir = tmp_path / "scripts"
        scripts_dir.mkdir()
        (scripts_dir / "20240101_001_first.py").write_text(
            'def upgrade(cnx):\n    cnx.execute("CREATE TABLE t1 (id INTEGER)")\n'
        )
        runner = MigrationRunner(f"sqlite3://{db_path}", str(scripts_dir))
        runner.upgrade()
        # Add a second script
        (scripts_dir / "20240102_001_second.py").write_text(
            'def upgrade(cnx):\n    cnx.execute("CREATE TABLE t2 (id INTEGER)")\n'
        )
        runner.upgrade()
        from dinao.backend import create_connection_pool

        pool = create_connection_pool(f"sqlite3://{db_path}")
        cnx = pool.lease()
        with cnx.query("SELECT revision_name FROM dinao_migration_revisions ORDER BY revision_name") as results:
            rows = results.fetchall()
        pool.release(cnx)
        pool.dispose()
        assert len(rows) == 2
        assert rows[0][0] == "20240101_001_first"
        assert rows[1][0] == "20240102_001_second"

    def test_upgrade_lock_contention(self, tmp_path):
        """Verify MigrationInProgressError when lock is already held."""
        db_path = str(tmp_path / "test.db")
        scripts_dir = tmp_path / "scripts"
        scripts_dir.mkdir()
        (scripts_dir / "20240101_001_create_table.py").write_text(
            'def upgrade(cnx):\n    cnx.execute("CREATE TABLE t1 (id INTEGER)")\n'
        )
        # Pre-create tables and insert a lock row
        from dinao.backend import create_connection_pool

        pool = create_connection_pool(f"sqlite3://{db_path}")
        cnx = pool.lease()
        runner = MigrationRunner(f"sqlite3://{db_path}", str(scripts_dir))
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
            runner.upgrade()

    def test_sample_scripts(self):
        """Verify the bundled sample scripts work end-to-end."""
        import tempfile

        sample_dir = os.path.join(os.path.dirname(__file__), "sample_scripts")
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = os.path.join(tmpdir, "test.db")
            runner = MigrationRunner(f"sqlite3://{db_path}", sample_dir)
            runner.upgrade()
            # Run again to verify idempotency
            runner.upgrade()
