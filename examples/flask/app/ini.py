import dbi
from dinao.backend import create_connection_pool

con_url = "postgresql+psycopg://test_user:test_pass@postgres:5432/test_db"
db_pool = create_connection_pool(con_url)
dbi.binder.pool = db_pool
dbi.init_db()
db_pool.dispose()
