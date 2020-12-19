# pip install dinao
# pip install psycopg2-binary
from dinao.backend import create_connection_pool
from dinao.binding import FunctionBinder

con_url = "postgresql://test_user:test_pass@localhost:5432/test_db"
db_pool = create_connection_pool(con_url)
binder = FunctionBinder(db_pool)


@binder.execute(
    "CREATE TABLE IF NOT EXISTS my_table ( "
    "  name VARCHAR(32) PRIMARY KEY, "
    "  value INTEGER DEFAULT 0"
    ")"
)
def make_table():
    pass


@binder.execute(
    "INSERT INTO my_table (name, value) VALUES(#{name}, #{value}) "
    "ON CONFLICT (name) DO UPDATE "
    "  SET value = #{value} "
    "WHERE my_table.name = #{name}"
)
def upsert(name: str, value: int):
    pass


@binder.query("SELECT name, value FROM my_table WHERE my_table.name LIKE #{search_term}")
def search(search_term: str):
    pass


@binder.transaction()
def populate():
    make_table()
    upsert("testing", 52)
    upsert("test", 39)
    upsert("other_thing", 20)


if __name__ == '__main__':
    populate()
    for row in search("test%"):
        n, v = row
        print(f"{n}: {v}")
