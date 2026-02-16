"""MariaDB-specific migration schema provider."""

from dinao.migration.schema.base import SchemaProviderBase


class MariaDBSchemaProvider(SchemaProviderBase):
    """Provides MariaDB-dialect DDL and DML for migration tracking tables."""

    def create_revisions_table(self) -> str:  # noqa: D102
        return (
            "CREATE TABLE IF NOT EXISTS dinao_migration_revisions ("
            "revision_name VARCHAR(255) NOT NULL, "
            "applied_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3), "
            "status VARCHAR(20) NOT NULL, "
            "error_type VARCHAR(255), "
            "error_message TEXT)"
        )

    def create_state_table(self) -> str:  # noqa: D102
        return (
            "CREATE TABLE IF NOT EXISTS dinao_migration_state ("
            "id INTEGER PRIMARY KEY CHECK (id = 1), "
            "status VARCHAR(20) NOT NULL, "
            "started_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3), "
            "completed_at TIMESTAMP(3) NULL, "
            "target_revision VARCHAR(255) NOT NULL, "
            "applied_count INTEGER NOT NULL DEFAULT 0, "
            "error_type VARCHAR(255), "
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
            "INSERT INTO dinao_migration_state (id, status, target_revision) "
            "SELECT 1, 'in_progress', #{target_revision} FROM DUAL "
            "WHERE NOT EXISTS (SELECT 1 FROM dinao_migration_state WHERE status = 'in_progress')"
        )

    def update_state_success(self) -> str:  # noqa: D102
        return (
            "UPDATE dinao_migration_state SET "
            "status = 'success', "
            "completed_at = CURRENT_TIMESTAMP(3), "
            "applied_count = #{applied_count} "
            "WHERE id = 1"
        )

    def update_state_error(self) -> str:  # noqa: D102
        return (
            "UPDATE dinao_migration_state SET "
            "status = 'error', "
            "completed_at = CURRENT_TIMESTAMP(3), "
            "error_type = #{error_type}, "
            "error_message = #{error_message} "
            "WHERE id = 1"
        )
