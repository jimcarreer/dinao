"""Shared base class for sync and async migration runners."""

import importlib.util
import inspect
import traceback
from typing import List, Optional, Set
from urllib.parse import urlparse

from dinao.backend import _parse_scheme
from dinao.migration.discovery import MigrationScript, ScriptDiscovery
from dinao.migration.errors import ScriptValidationError
from dinao.migration.schema import SchemaProviderBase, get_schema_provider


class MigrationRunnerBase:
    """Base class providing shared logic for migration runners.

    Handles URL parsing, script discovery, module loading, and
    computation of pending migrations.
    """

    def __init__(self, db_url: str, script_dir: str, pattern: Optional[str] = None):
        """Construct a migration runner base.

        :param db_url: database connection URL
        :param script_dir: path to the directory containing migration scripts
        :param pattern: optional regex pattern for migration filenames
        """
        self._db_url = db_url
        parsed = urlparse(db_url)
        backend, _engine, _mode = _parse_scheme(parsed.scheme)
        self._backend = backend
        self._discovery = ScriptDiscovery(script_dir, pattern)
        self._schema_provider = get_schema_provider(backend)

    @property
    def schema_provider(self) -> SchemaProviderBase:
        """Return the schema provider for this runner's backend.

        :returns: the ``SchemaProviderBase`` instance
        """
        return self._schema_provider

    @staticmethod
    def _load_module(script: MigrationScript):
        """Load a Python module from a migration script path.

        :param script: the migration script to load
        :returns: the loaded module object
        """
        spec = importlib.util.spec_from_file_location(script.name, script.path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    @staticmethod
    def _validate_upgrade_sync(module, script: MigrationScript):
        """Validate that the module has a callable sync upgrade function.

        :param module: the loaded module
        :param script: the migration script metadata
        :raises ScriptValidationError: if the upgrade function is missing or not callable
        """
        upgrade = getattr(module, "upgrade", None)
        if upgrade is None or not callable(upgrade):
            raise ScriptValidationError(f"Migration script '{script.name}' must define a callable 'upgrade' function")

    @staticmethod
    def _validate_upgrade_async(module, script: MigrationScript):
        """Validate that the module has a coroutine upgrade function.

        :param module: the loaded module
        :param script: the migration script metadata
        :raises ScriptValidationError: if the upgrade function is missing or not a coroutine
        """
        upgrade = getattr(module, "upgrade", None)
        if upgrade is None or not inspect.iscoroutinefunction(upgrade):
            raise ScriptValidationError(f"Migration script '{script.name}' must define an async 'upgrade' function")

    @staticmethod
    def _format_error(exc: Exception) -> str:
        """Format an exception with its full traceback as a string.

        :param exc: the exception to format
        :returns: the formatted traceback string
        """
        return "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))

    @staticmethod
    def _compute_pending(scripts: List[MigrationScript], applied: Set[str]) -> List[MigrationScript]:
        """Return scripts that have not yet been applied.

        :param scripts: all discovered scripts in order
        :param applied: set of revision names already applied
        :returns: list of pending migration scripts
        """
        return [s for s in scripts if s.name not in applied]
