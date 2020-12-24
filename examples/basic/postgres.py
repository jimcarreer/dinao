import dbi
from dinao.backend import create_connection_pool

if __name__ == "__main__":
    con_url = "postgresql://test_user:test_pass@localhost:5432/test_db"
    db_pool = create_connection_pool(con_url)
    dbi.binder.pool = db_pool
    dbi.populate()
    for n, v in dbi.search("test%", {"limit": 10, "offset": 0}):
        print(f"{n}: {v}")
