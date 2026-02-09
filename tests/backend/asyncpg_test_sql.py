"""SQL statements to be used in testing asyncpg implementations."""

from tests.backend.postgres_test_sql import CREATE_TABLE

SIMPLE_INSERT = "INSERT INTO all_the_things (pk_col, col_uuid, col_bigint, col_integer) VALUES($1, $2, $3, $4)"

SIMPLE_SELECT = """SELECT pk_col as my_pk_col, col_uuid as some_uuid, col_bigint, col_integer
FROM all_the_things
WHERE col_bigint > $1;
"""

__all__ = ["CREATE_TABLE", "SIMPLE_INSERT", "SIMPLE_SELECT"]
