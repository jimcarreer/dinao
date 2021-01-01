import dbi
from dinao.backend import create_connection_pool

if __name__ == '__main__':
    con_url = "sqlite3:///tmp/example.db"
    db_pool = create_connection_pool(con_url)
    dbi.binder.pool = db_pool
    dbi.populate()
    for row in dbi.search("test%", {"limit": 10, "offset": 0}):
        n, v = row
        print(f"{n}: {v}")
