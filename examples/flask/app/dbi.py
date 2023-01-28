import dataclasses

from dinao.binding import FunctionBinder

binder = FunctionBinder()


@dataclasses.dataclass
class MyModel:

    name: str
    value: int


@binder.execute(
    "CREATE TABLE IF NOT EXISTS data ( "
    "  name VARCHAR(256) PRIMARY KEY, "
    "  value INTEGER DEFAULT 0 "
    ")"
)
def init_db():
    pass


@binder.execute(
    "INSERT INTO data (name, value) VALUES(#{name}, #{value}) "
    "ON CONFLICT (name) DO UPDATE "
    "  SET value = #{value} "
    "WHERE data.name = #{name}"
)
def upsert(name: str, value: int) -> int:
    pass


@binder.query(
    "SELECT name, value FROM data WHERE !{search_column} LIKE #{search_term} "
    "ORDER BY data.name LIMIT #{page.limit} OFFSET #{page.offset} "
)
def search(search_column: str, search_term: str, page: dict) -> list[MyModel]:
    pass


@binder.transaction()
def sum_for(search_term: str, page_size: int = 5) -> dict:
    page = {"limit": page_size, "offset": 0}
    entries = search("name", search_term, page)
    summed = 0
    rows = 0
    pages = 0
    while entries:
        pages += 1
        for model in entries:
            summed += model.value
            rows += 1
        page["offset"] += page["limit"]
        entries = search("name", search_term, page)
    return {"summed": summed, "rows": rows, "pages": pages, "page_size": page_size}
