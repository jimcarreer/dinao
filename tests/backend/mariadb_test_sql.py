"""SQL statements to be used in testing postgres implementations."""

TERMINATE_DB_CONNS = """SELECT
CONCAT('KILL ', id, ';')
FROM INFORMATION_SCHEMA.PROCESSLIST
"""

CREATE_TABLE = """CREATE TABLE all_the_things (
    pk_col             UUID PRIMARY KEY,
    col_tiny_uint      TINYINT UNSIGNED,
    col_tiny_int       TINYINT,
    col_boolean        BOOLEAN,
    col_small_uint     SMALLINT UNSIGNED,
    col_small_int      SMALLINT,
    col_medium_uint    MEDIUMINT UNSIGNED,
    col_medium_int     MEDIUMINT,
    col_uint           INT UNSIGNED,
    col_int            INT,
    col_big_unit       BIGINT UNSIGNED,
    col_big_int        BIGINT,
    col_decimal        DECIMAL,
    col_float          FLOAT,
    col_double         DOUBLE,
    col_bit            BIT,
    col_uuid           UUID
);
"""

SIMPLE_INSERT = "INSERT INTO all_the_things (pk_col, col_uuid, col_big_int, col_int) VALUES(?, ?, ?, ?)"

SIMPLE_SELECT = """SELECT pk_col as my_pk_col, col_uuid as some_uuid, col_big_int, col_int
FROM all_the_things
WHERE col_big_int > ?
ORDER BY col_big_int;
"""
