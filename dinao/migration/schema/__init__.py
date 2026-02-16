"""Per-dialect DDL and DML providers for migration tracking tables."""

from dinao.migration.schema.base import SchemaProviderBase
from dinao.migration.schema.mariadb import MariaDBSchemaProvider
from dinao.migration.schema.mysql import MySQLSchemaProvider
from dinao.migration.schema.postgres import PostgresSchemaProvider
from dinao.migration.schema.sqlite import SQLiteSchemaProvider

_PROVIDER_MAP = {
    "sqlite3": SQLiteSchemaProvider,
    "postgresql": PostgresSchemaProvider,
    "mariadb": MariaDBSchemaProvider,
    "mysql": MySQLSchemaProvider,
}


def get_schema_provider(backend: str) -> SchemaProviderBase:
    """Return the schema provider for the given backend name.

    :param backend: the database backend identifier (e.g. ``"sqlite3"``, ``"postgresql"``)
    :returns: a ``SchemaProviderBase`` instance for the dialect
    :raises ValueError: if the backend is not supported
    """
    provider_class = _PROVIDER_MAP.get(backend)
    if provider_class is None:
        raise ValueError(f"Unsupported backend for migrations: '{backend}'")
    return provider_class()


__all__ = [
    "MariaDBSchemaProvider",
    "MySQLSchemaProvider",
    "PostgresSchemaProvider",
    "SQLiteSchemaProvider",
    "SchemaProviderBase",
    "get_schema_provider",
]
