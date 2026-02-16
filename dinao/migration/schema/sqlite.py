"""SQLite-specific migration schema provider."""

from dinao.migration.schema.base import SchemaProviderBase


class SQLiteSchemaProvider(SchemaProviderBase):
    """Provides SQLite-dialect DDL and DML for migration tracking tables."""

    def create_revisions_table(self) -> str:  # noqa: D102
        return (
            "CREATE TABLE IF NOT EXISTS dinao_migration_revisions ("
            "revision_name TEXT NOT NULL, "
            "applied_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f', 'now')), "
            "status TEXT NOT NULL, "
            "error_type TEXT, "
            "error_message TEXT)"
        )

    def create_state_table(self) -> str:  # noqa: D102
        return (
            "CREATE TABLE IF NOT EXISTS dinao_migration_state ("
            "id INTEGER PRIMARY KEY CHECK (id = 1), "
            "status TEXT NOT NULL, "
            "started_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f', 'now')), "
            "completed_at TEXT, "
            "target_revision TEXT NOT NULL, "
            "applied_count INTEGER NOT NULL DEFAULT 0, "
            "error_type TEXT, "
            "error_message TEXT)"
        )

    def select_applied_revisions(self) -> str:  # noqa: D102
        return "SELECT revision_name FROM dinao_migration_revisions WHERE status = 'success'"

    def insert_revision(self) -> str:  # noqa: D102
        return (
            "INSERT INTO dinao_migration_revisions (revision_name, status, error_type, error_message) "
            "VALUES (#{revision_name}, #{status}, #{error_type}, #{error_message})"
        )

    def acquire_lock(self) -> str:  # noqa: D102
        return (
            "INSERT OR IGNORE INTO dinao_migration_state (id, status, target_revision) "
            "SELECT 1, 'in_progress', #{target_revision} "
            "WHERE NOT EXISTS (SELECT 1 FROM dinao_migration_state WHERE status = 'in_progress')"
        )

    def update_state_success(self) -> str:  # noqa: D102
        return (
            "UPDATE dinao_migration_state SET "
            "status = 'success', "
            "completed_at = strftime('%Y-%m-%dT%H:%M:%f', 'now'), "
            "applied_count = #{applied_count} "
            "WHERE id = 1"
        )

    def update_state_error(self) -> str:  # noqa: D102
        return (
            "UPDATE dinao_migration_state SET "
            "status = 'error', "
            "completed_at = strftime('%Y-%m-%dT%H:%M:%f', 'now'), "
            "error_type = #{error_type}, "
            "error_message = #{error_message} "
            "WHERE id = 1"
        )
