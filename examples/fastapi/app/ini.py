import asyncio
import os

from dinao.migration import AsyncMigrationRunner

con_url = "postgresql+asyncpg://test_user:test_pass@postgres:5432/test_db"
script_dir = os.path.join(os.path.dirname(__file__), "migrations")
runner = AsyncMigrationRunner(con_url, script_dir)

asyncio.run(runner.upgrade())
