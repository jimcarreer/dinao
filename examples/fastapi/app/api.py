import time
from contextlib import asynccontextmanager

from dinao.backend import create_connection_pool
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

import dbi


@asynccontextmanager
async def lifespan(application: FastAPI):
    con_url = "postgresql+asyncpg://test_user:test_pass@postgres:5432/test_db"
    db_pool = create_connection_pool(con_url)
    print("Setting pool for binder.")
    dbi.binder.pool = db_pool
    yield
    print("Disposing pool.")
    await db_pool.dispose()


app = FastAPI(title="dinao_fastapi_example", lifespan=lifespan)


def make_error(error: str, status: int = 400):
    return JSONResponse({"error": error}, status_code=status)


@app.middleware("http")
async def log_requests(request: Request, call_next):
    started = time.time_ns() / 1000000.0
    response = await call_next(request)
    finished = time.time_ns() / 1000000.0
    elapsed = round(finished - started, 2)
    print(f"Handled {request.method} {request.url.path} ({response.status_code}) in {elapsed}ms")
    # Raise Cain if this happens.
    active_cnx = dbi.binder._context_store.get()
    assert active_cnx is None, "There should never be an active connection once the request is done."
    return response


@app.get("/")
async def listing(page: int = 1, size: int = 10, search: str = "%"):
    if page < 1:
        return make_error("Bad page number")
    if size < 1:
        return make_error("Bad page size")
    page_num = page - 1
    res = await dbi.search("name", search, {"offset": (page_num * size), "limit": size})
    return JSONResponse({"results": [m.model_dump() for m in res]})


@app.post("/")
async def update(request: Request):
    payload = await request.json()
    if not payload:
        return make_error("Missing JSON payload")
    updates = {"name": payload.get("name"), "value": payload.get("value")}
    if not all(v is not None for _, v in updates.items()):
        return make_error("Missing name or value in payload")
    if not isinstance(updates["name"], str):
        return make_error("Invalid value for name, must be string")
    if not isinstance(updates["value"], int):
        return make_error("Invalid value for value, must be integer")
    updated = await dbi.upsert(**updates)
    return JSONResponse({"updated": updated})


@app.get("/summed")
async def summed(search: str = "%", size: int = 10):
    return JSONResponse(await dbi.sum_for(search, size))
