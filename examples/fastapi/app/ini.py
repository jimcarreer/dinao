import asyncio

import dbi
from dinao.backend import create_connection_pool

con_url = "postgresql+psycopg+async://test_user:test_pass@postgres:5432/test_db"
db_pool = create_connection_pool(con_url)
dbi.binder.pool = db_pool


async def run_init():
    await dbi.init_db()
    await db_pool.dispose()


asyncio.run(run_init())
