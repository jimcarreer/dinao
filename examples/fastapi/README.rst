FastAPI DINAO Example
=====================

A simple example of how to use DINAO with FastAPI, using the async engine
(psycopg v3) and Pydantic models.  To run this example you need:

* docker
* docker-compose

Additionally you will want to install the python library ``requests`` to be
able to run ``tester.py``.

You simply need to run:

.. code-block::

    $ ./build.sh
    $ docker compose up
    $ pip install requests
    $ python3 tester.py


Special Notes
*************

This example uses ``AsyncFunctionBinder`` with the ``psycopg`` (v3) async
backend.  The connection URL uses the ``+async`` suffix to select the async
connection pool::

    postgresql+psycopg+async://user:pass@host:port/dbname

FastAPI is served via gunicorn using ``uvicorn.workers.UvicornWorker``,
which provides an ASGI server capable of running async request handlers.
Unlike the Flask / WSGI example, async workers handle concurrency within
each process, so fewer worker processes are needed.

The database pool is initialized in FastAPI's ``lifespan`` context manager,
which ensures proper setup on startup and cleanup on shutdown.
