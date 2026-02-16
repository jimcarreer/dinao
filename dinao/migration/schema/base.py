"""Abstract base class for per-dialect migration schema providers."""

from abc import ABC, abstractmethod


class SchemaProviderBase(ABC):
    """Abstract base providing DDL and DML templates for migration tracking tables.

    Subclasses implement dialect-specific DDL for table creation and
    dialect-specific DML for lock acquisition, while shared DML methods
    are provided here.
    """

    @abstractmethod
    def create_revisions_table(self) -> str:
        """Return DDL to create the dinao_migration_revisions table.

        :returns: a DDL statement string
        """
        pass  # pragma: no cover

    @abstractmethod
    def create_state_table(self) -> str:
        """Return DDL to create the dinao_migration_state table.

        :returns: a DDL statement string
        """
        pass  # pragma: no cover

    @abstractmethod
    def select_applied_revisions(self) -> str:
        """Return DML to select all applied revision names.

        :returns: a SELECT statement string
        """
        pass  # pragma: no cover

    @abstractmethod
    def insert_revision(self) -> str:
        """Return DML to insert a revision record.

        The template must accept ``#{revision_name}``, ``#{status}``,
        ``#{error_type}``, and ``#{error_message}`` parameters.

        :returns: an INSERT statement string
        """
        pass  # pragma: no cover

    @abstractmethod
    def acquire_lock(self) -> str:
        """Return DML to insert a state row only when no in-progress row exists.

        The template must accept ``#{target_revision}`` parameter.
        The statement should affect zero rows when a lock is already held.

        :returns: an INSERT statement string
        """
        pass  # pragma: no cover

    @abstractmethod
    def update_state_success(self) -> str:
        """Return DML to update the state row on successful completion.

        The template must accept ``#{applied_count}`` parameter.

        :returns: an UPDATE statement string
        """
        pass  # pragma: no cover

    @abstractmethod
    def update_state_error(self) -> str:
        """Return DML to update the state row on error.

        The template must accept ``#{error_type}`` and ``#{error_message}``
        parameters.

        :returns: an UPDATE statement string
        """
        pass  # pragma: no cover

    def delete_completed_state(self) -> str:
        """Return DML to delete completed state rows before acquiring a new lock.

        :returns: a DELETE statement string
        """
        return "DELETE FROM dinao_migration_state WHERE status != 'in_progress'"
