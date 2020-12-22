# pip install dinao
# pip install psycopg2-binary
from dinao.backend import create_connection_pool
from dinao.binding import FunctionBinder

con_url = "postgresql://test_user:test_pass@localhost:5432/test_db?pool_threaded=True&pool_max_conn=30"
db_pool = create_connection_pool(con_url)
binder = FunctionBinder(db_pool)


@binder.execute(
    "CREATE TABLE IF NOT EXISTS data ( "
    "  name VARCHAR(256) PRIMARY KEY, "
    "  value INTEGER DEFAULT 0"
    ")"
)
def make_table():
    pass


@binder.execute(
    "INSERT INTO data (name, value) VALUES(#{name}, #{value}) "
    "ON CONFLICT (name) DO UPDATE "
    "  SET value = #{value} "
    "WHERE data.name = #{name}"
)
def upsert(name: str, value: int):
    pass


@binder.query(
    "SELECT name, value FROM data WHERE data.name LIKE #{search_term} "
    "ORDER BY data.name LIMIT #{page.limit} OFFSET #{page.offset} "
)
def search(search_term: str, page: dict):
    pass


@binder.transaction()
def sum_for(search_term: str, page_size: int = 5) -> dict:
    page = {"limit": page_size, "offset": 0}
    entries = search(search_term, page)
    summed = 0
    rows = 0
    pages = 0
    while entries:
        pages += 1
        for row in entries:
            summed += row[1]
            rows += 1
        page["offset"] += page["limit"]
        entries = search(search_term, page)
    return {"summed": summed, "rows": rows, "pages": pages, "page_size": page_size}


@binder.transaction()
def populate():
    make_table()
    upsert("testing", 52)
    upsert("test", 39)
    upsert("other_thing", 20)


if __name__ == '__main__':
    populate()
    for n, v in search("test%"):
        print(f"{n}: {v}")
