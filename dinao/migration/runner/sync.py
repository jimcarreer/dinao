"""Synchronous migration runner implementation."""

from dinao.backend import create_connection_pool
from dinao.binding.templating import Template
from dinao.migration.connection import MigrationConnection
from dinao.migration.errors import MigrationInProgressError, RevisionError
from dinao.migration.runner.base import MigrationRunnerBase


class MigrationRunner(MigrationRunnerBase):
    """Runs migration scripts synchronously against a database.

    Creates a connection pool, acquires a lock to prevent concurrent
    migration runs, then executes pending scripts in order. Each script
    is run within its own transaction.
    """

    def upgrade(self):
        """Discover and apply all pending migration scripts.

        :raises MigrationInProgressError: if another process holds the migration lock
        :raises RevisionError: if a migration script fails
        :raises ScriptValidationError: if a script is missing a valid upgrade function
        :raises DiscoveryError: if the script directory is invalid
        """
        pool = create_connection_pool(self._db_url)
        cnx = pool.lease()
        try:
            self._run_upgrade(cnx, pool)
        finally:
            pool.release(cnx)
            pool.dispose()

    def _run_upgrade(self, cnx, pool):
        """Execute the upgrade flow on the given connection.

        :param cnx: a leased database connection
        :param pool: the connection pool for mung symbol access
        """
        sp = self._schema_provider
        self._create_tables(cnx, sp)
        scripts = self._discovery.discover()
        applied = self._get_applied_revisions(cnx, pool, sp)
        pending = self._compute_pending(scripts, applied)
        if not pending:
            return
        self._acquire_migration_lock(cnx, pool, sp, pending[-1].name)
        self._apply_pending_scripts(cnx, pool, sp, pending)

    def _create_tables(self, cnx, sp):
        """Create migration tracking tables if they do not exist.

        :param cnx: database connection
        :param sp: schema provider
        """
        cnx.execute(sp.create_revisions_table(), commit=True)
        cnx.execute(sp.create_state_table(), commit=True)

    def _get_applied_revisions(self, cnx, pool, sp):
        """Query the set of already-applied revision names.

        :param cnx: database connection
        :param pool: connection pool for mung symbol access
        :param sp: schema provider
        :returns: set of applied revision name strings
        """
        applied = set()
        with cnx.query(sp.select_applied_revisions()) as results:
            for row in results.fetchall():
                applied.add(row[0])
        return applied

    def _acquire_migration_lock(self, cnx, pool, sp, target_revision):
        """Delete old state rows and acquire the migration lock.

        :param cnx: database connection
        :param pool: connection pool for mung symbol access
        :param sp: schema provider
        :param target_revision: name of the last pending revision
        :raises MigrationInProgressError: if the lock is already held
        """
        cnx.execute(sp.delete_completed_state(), commit=True)
        template = Template(sp.acquire_lock())
        sql, params = template.render(pool.mung_symbol, {"target_revision": target_revision})
        affected = cnx.execute(sql, params, commit=True)
        if affected == 0:
            raise MigrationInProgressError("Another migration is already in progress")

    def _apply_pending_scripts(self, cnx, pool, sp, pending):
        """Apply each pending script in order within individual transactions.

        :param cnx: database connection
        :param pool: connection pool for mung symbol access
        :param sp: schema provider
        :param pending: list of pending migration scripts
        """
        applied_count = 0
        for script in pending:
            try:
                self._apply_single_script(cnx, pool, sp, script)
                applied_count += 1
            except Exception as exc:
                self._handle_script_error(cnx, pool, sp, script, exc)
                raise RevisionError(f"Migration '{script.name}' failed: {exc}") from exc
        self._update_state_success(cnx, pool, sp, applied_count)

    def _apply_single_script(self, cnx, pool, sp, script):
        """Load, validate, and execute a single migration script.

        :param cnx: database connection
        :param pool: connection pool for mung symbol access
        :param sp: schema provider
        :param script: the migration script to apply
        """
        cnx.autocommit = False
        module = self._load_module(script)
        self._validate_upgrade_sync(module, script)
        migration_cnx = MigrationConnection(cnx, pool)
        module.upgrade(migration_cnx)
        self._insert_revision(cnx, pool, sp, script.name, "success", None, None)
        cnx.commit()
        cnx.autocommit = True

    def _handle_script_error(self, cnx, pool, sp, script, exc):
        """Record the error state after a script failure.

        :param cnx: database connection
        :param pool: connection pool for mung symbol access
        :param sp: schema provider
        :param script: the failed migration script
        :param exc: the exception that occurred
        """
        cnx.rollback()
        cnx.autocommit = True
        error_type = type(exc).__name__
        error_message = self._format_error(exc)
        self._insert_revision(cnx, pool, sp, script.name, "error", error_type, error_message)
        self._update_state_error(cnx, pool, sp, error_type, error_message)

    def _insert_revision(self, cnx, pool, sp, revision_name, status, error_type, error_message):
        """Insert a revision record into the tracking table.

        :param cnx: database connection
        :param pool: connection pool for mung symbol access
        :param sp: schema provider
        :param revision_name: the revision name
        :param status: 'success' or 'error'
        :param error_type: exception class name or None
        :param error_message: formatted traceback or None
        """
        template = Template(sp.insert_revision())
        kwargs = {
            "revision_name": revision_name,
            "status": status,
            "error_type": error_type,
            "error_message": error_message,
        }
        sql, params = template.render(pool.mung_symbol, kwargs)
        cnx.execute(sql, params, commit=True)

    def _update_state_success(self, cnx, pool, sp, applied_count):
        """Update the state row to indicate successful completion.

        :param cnx: database connection
        :param pool: connection pool for mung symbol access
        :param sp: schema provider
        :param applied_count: number of revisions applied
        """
        template = Template(sp.update_state_success())
        sql, params = template.render(pool.mung_symbol, {"applied_count": applied_count})
        cnx.execute(sql, params, commit=True)

    def _update_state_error(self, cnx, pool, sp, error_type, error_message):
        """Update the state row to indicate an error.

        :param cnx: database connection
        :param pool: connection pool for mung symbol access
        :param sp: schema provider
        :param error_type: exception class name
        :param error_message: formatted traceback
        """
        template = Template(sp.update_state_error())
        kwargs = {"error_type": error_type, "error_message": error_message}
        sql, params = template.render(pool.mung_symbol, kwargs)
        cnx.execute(sql, params, commit=True)
