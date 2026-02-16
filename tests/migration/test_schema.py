"""Tests for migration schema providers."""

from dinao.migration.schema import (
    MariaDBSchemaProvider,
    MySQLSchemaProvider,
    PostgresSchemaProvider,
    SQLiteSchemaProvider,
    get_schema_provider,
)

import pytest


class TestGetSchemaProvider:
    """Tests for the get_schema_provider factory function."""

    def test_sqlite3(self):
        """Verify sqlite3 backend returns SQLiteSchemaProvider."""
        provider = get_schema_provider("sqlite3")
        assert isinstance(provider, SQLiteSchemaProvider)

    def test_postgresql(self):
        """Verify postgresql backend returns PostgresSchemaProvider."""
        provider = get_schema_provider("postgresql")
        assert isinstance(provider, PostgresSchemaProvider)

    def test_mariadb(self):
        """Verify mariadb backend returns MariaDBSchemaProvider."""
        provider = get_schema_provider("mariadb")
        assert isinstance(provider, MariaDBSchemaProvider)

    def test_mysql(self):
        """Verify mysql backend returns MySQLSchemaProvider."""
        provider = get_schema_provider("mysql")
        assert isinstance(provider, MySQLSchemaProvider)

    def test_unsupported_backend(self):
        """Verify ValueError is raised for unsupported backends."""
        with pytest.raises(ValueError, match="Unsupported backend"):
            get_schema_provider("oracle")


class TestSQLiteSchemaProvider:
    """Tests for SQLite-specific schema SQL."""

    def setup_method(self):
        """Create a SQLite schema provider for each test."""
        self.provider = SQLiteSchemaProvider()

    def test_create_revisions_table(self):
        """Verify DDL creates dinao_migration_revisions with SQLite types."""
        ddl = self.provider.create_revisions_table()
        assert "CREATE TABLE IF NOT EXISTS dinao_migration_revisions" in ddl
        assert "TEXT" in ddl
        assert "strftime" in ddl

    def test_create_state_table(self):
        """Verify DDL creates dinao_migration_state with CHECK constraint."""
        ddl = self.provider.create_state_table()
        assert "CREATE TABLE IF NOT EXISTS dinao_migration_state" in ddl
        assert "CHECK (id = 1)" in ddl

    def test_select_applied_revisions(self):
        """Verify SELECT returns revision names with success status."""
        sql = self.provider.select_applied_revisions()
        assert "SELECT revision_name FROM dinao_migration_revisions" in sql
        assert "status = 'success'" in sql

    def test_insert_revision(self):
        """Verify INSERT template has required parameters."""
        sql = self.provider.insert_revision()
        assert "#{revision_name}" in sql
        assert "#{status}" in sql
        assert "#{error_type}" in sql
        assert "#{error_message}" in sql

    def test_acquire_lock(self):
        """Verify lock SQL uses INSERT OR IGNORE for SQLite."""
        sql = self.provider.acquire_lock()
        assert "INSERT OR IGNORE" in sql
        assert "#{target_revision}" in sql

    def test_update_state_success(self):
        """Verify success update template has applied_count parameter."""
        sql = self.provider.update_state_success()
        assert "#{applied_count}" in sql
        assert "status = 'success'" in sql

    def test_update_state_error(self):
        """Verify error update template has error parameters."""
        sql = self.provider.update_state_error()
        assert "#{error_type}" in sql
        assert "#{error_message}" in sql
        assert "status = 'error'" in sql

    def test_delete_completed_state(self):
        """Verify delete removes non-in-progress rows."""
        sql = self.provider.delete_completed_state()
        assert "DELETE FROM dinao_migration_state" in sql
        assert "status != 'in_progress'" in sql


class TestPostgresSchemaProvider:
    """Tests for PostgreSQL-specific schema SQL."""

    def setup_method(self):
        """Create a PostgreSQL schema provider for each test."""
        self.provider = PostgresSchemaProvider()

    def test_create_revisions_table(self):
        """Verify DDL uses PostgreSQL types."""
        ddl = self.provider.create_revisions_table()
        assert "CREATE TABLE IF NOT EXISTS dinao_migration_revisions" in ddl
        assert "VARCHAR" in ddl
        assert "TIMESTAMPTZ" in ddl

    def test_create_state_table(self):
        """Verify DDL uses PostgreSQL types and CHECK constraint."""
        ddl = self.provider.create_state_table()
        assert "CREATE TABLE IF NOT EXISTS dinao_migration_state" in ddl
        assert "CHECK (id = 1)" in ddl
        assert "TIMESTAMPTZ" in ddl

    def test_select_applied_revisions(self):
        """Verify SELECT returns revision names with success status."""
        sql = self.provider.select_applied_revisions()
        assert "SELECT revision_name FROM dinao_migration_revisions" in sql
        assert "status = 'success'" in sql

    def test_insert_revision(self):
        """Verify INSERT template has required parameters."""
        sql = self.provider.insert_revision()
        assert "#{revision_name}" in sql
        assert "#{status}" in sql

    def test_acquire_lock(self):
        """Verify lock SQL uses standard WHERE NOT EXISTS for PostgreSQL."""
        sql = self.provider.acquire_lock()
        assert "INSERT INTO dinao_migration_state" in sql
        assert "WHERE NOT EXISTS" in sql
        assert "#{target_revision}" in sql

    def test_update_state_success(self):
        """Verify success update uses NOW()."""
        sql = self.provider.update_state_success()
        assert "NOW()" in sql
        assert "#{applied_count}" in sql

    def test_update_state_error(self):
        """Verify error update uses NOW()."""
        sql = self.provider.update_state_error()
        assert "NOW()" in sql


class TestMariaDBSchemaProvider:
    """Tests for MariaDB-specific schema SQL."""

    def setup_method(self):
        """Create a MariaDB schema provider for each test."""
        self.provider = MariaDBSchemaProvider()

    def test_create_revisions_table(self):
        """Verify DDL uses MariaDB TIMESTAMP type."""
        ddl = self.provider.create_revisions_table()
        assert "CREATE TABLE IF NOT EXISTS dinao_migration_revisions" in ddl
        assert "TIMESTAMP(3)" in ddl
        assert "CURRENT_TIMESTAMP(3)" in ddl

    def test_create_state_table(self):
        """Verify DDL uses MariaDB types."""
        ddl = self.provider.create_state_table()
        assert "TIMESTAMP(3)" in ddl
        assert "CHECK (id = 1)" in ddl

    def test_select_applied_revisions(self):
        """Verify SELECT returns revision names with success status."""
        sql = self.provider.select_applied_revisions()
        assert "SELECT revision_name FROM dinao_migration_revisions" in sql
        assert "status = 'success'" in sql

    def test_insert_revision(self):
        """Verify INSERT template has required parameters."""
        sql = self.provider.insert_revision()
        assert "#{revision_name}" in sql
        assert "#{status}" in sql

    def test_acquire_lock(self):
        """Verify lock SQL uses FROM DUAL for MariaDB."""
        sql = self.provider.acquire_lock()
        assert "FROM DUAL" in sql
        assert "WHERE NOT EXISTS" in sql

    def test_update_state_success(self):
        """Verify success update uses CURRENT_TIMESTAMP(3)."""
        sql = self.provider.update_state_success()
        assert "CURRENT_TIMESTAMP(3)" in sql

    def test_update_state_error(self):
        """Verify error update uses CURRENT_TIMESTAMP(3)."""
        sql = self.provider.update_state_error()
        assert "CURRENT_TIMESTAMP(3)" in sql


class TestMySQLSchemaProvider:
    """Tests for MySQL schema provider."""

    def test_inherits_mariadb(self):
        """Verify MySQLSchemaProvider inherits from MariaDBSchemaProvider."""
        assert issubclass(MySQLSchemaProvider, MariaDBSchemaProvider)

    def test_produces_same_sql(self):
        """Verify MySQL and MariaDB produce identical SQL."""
        mysql = MySQLSchemaProvider()
        maria = MariaDBSchemaProvider()
        assert mysql.create_revisions_table() == maria.create_revisions_table()
        assert mysql.create_state_table() == maria.create_state_table()
        assert mysql.acquire_lock() == maria.acquire_lock()
        assert mysql.insert_revision() == maria.insert_revision()
        assert mysql.select_applied_revisions() == maria.select_applied_revisions()
        assert mysql.update_state_success() == maria.update_state_success()
        assert mysql.update_state_error() == maria.update_state_error()
        assert mysql.delete_completed_state() == maria.delete_completed_state()
