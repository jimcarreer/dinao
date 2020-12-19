"""SQL statements to be used in testing sqlite implementations."""

CREATE_TABLE = """CREATE TABLE all_the_things (
    pk_col      TEXT,
    col_integer INTEGER,
    col_bigint  INTEGER,
    col_text    TEXT,
    col_blob    BLOB,
    col_real    REAL,
    col_numeric NUMERIC
);
"""

SIMPLE_INSERT = "INSERT INTO all_the_things (pk_col, col_text, col_bigint, col_integer) VALUES(?, ?, ?, ?)"

SIMPLE_SELECT = """SELECT pk_col as my_pk_col, col_text as some_uuid, col_bigint, col_integer
FROM all_the_things
WHERE col_bigint > ?;
"""
