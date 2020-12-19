"""SQL statements to be used in testing postgres implementations."""

TERMINATE_DB_CONNS = """SELECT pg_terminate_backend(pg_stat_activity.pid)
FROM pg_stat_activity
WHERE pg_stat_activity.datname = %s
  AND pid <> pg_backend_pid();
"""

CREATE_TABLE = """CREATE TABLE all_the_things (
    pk_col             UUID PRIMARY KEY,
    col_bigint         BIGINT,
    col_bigserial      BIGSERIAL,
    col_bit            BIT,
    col_vbit           VARBIT(8),
    col_boolean        BOOLEAN,
    col_box            BOX,
    col_bytea          BYTEA,
    col_character      CHAR(8),
    col_vcharacter     VARCHAR(8),
    col_cidr           CIDR,
    col_circle         CIRCLE,
    col_date           DATE,
    col_float8         FLOAT8,
    col_inet           INET,
    col_integer        INTEGER,
    col_interval       INTERVAL,
    col_json           JSON,
    col_jsonb          JSONB,
    col_line           LINE,
    col_lseg           LSEG,
    col_macaddr        MACADDR,
    col_money          MONEY,
    col_numeric        NUMERIC,
    col_path           PATH,
    col_pg_lsn         PG_LSN,
    col_point          POINT,
    col_polygon        POLYGON,
    col_real           REAL,
    col_smallint       SMALLINT,
    col_smallserial    SMALLSERIAL,
    col_serial         SERIAL,
    col_text           TEXT,
    col_time           TIME,
    col_timetz         TIMETZ,
    col_timestamp      TIMESTAMP,
    col_timestamptz    TIMESTAMPTZ,
    col_tsquery        TSQUERY,
    col_tsvector       TSVECTOR,
    col_txid_snapshot  TXID_SNAPSHOT,
    col_uuid           UUID,
    col_xml            XML
);
"""

SIMPLE_INSERT = "INSERT INTO all_the_things (pk_col, col_uuid, col_bigint, col_integer) VALUES(%s, %s, %s, %s)"

SIMPLE_SELECT = """SELECT pk_col as my_pk_col, col_uuid as some_uuid, col_bigint, col_integer
FROM all_the_things
WHERE col_bigint > %s;
"""
