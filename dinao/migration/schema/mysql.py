"""MySQL-specific migration schema provider."""

from dinao.migration.schema.mariadb import MariaDBSchemaProvider


class MySQLSchemaProvider(MariaDBSchemaProvider):
    """Provides MySQL-dialect DDL and DML for migration tracking tables.

    MySQL and MariaDB share the same SQL dialect for the operations
    required by the migration system, so this class inherits everything
    from ``MariaDBSchemaProvider``.
    """

    pass
