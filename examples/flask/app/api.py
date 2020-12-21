import atexit
import time
import signal

from flask import Flask,  g, request, jsonify, Response

import dbi

app = Flask('dinao_flask_example')


def mk_error(error: str, status: int = 400):
    return jsonify({"error": error}), status


@app.before_first_request
def register_shutdowns():
    print("Registering shutdown for cleaning up pool.")
    atexit.register(dbi.db_pool.dispose)
    signal.signal(signal.SIGTERM, dbi.db_pool.dispose)
    signal.signal(signal.SIGINT, dbi.db_pool.dispose)


@app.before_request
def before_request():
    g.started = time.time_ns() / 1000000.0


@app.after_request
def after_request(response: Response):
    finished = time.time_ns() / 1000000.0
    elapsed = round(finished - g.started, 2)
    status = f"{response.status_code} {response.status}"
    body = response.get_data(as_text=True)
    body = f"Body:\n{body.strip()}" if body else ""
    print(f"Handled {request.method} {request.path} ({status}) in {elapsed}ms {body}")
    # Raise Cain if this happens.
    assert dbi.binder._active_cnx is None, "There should never be an active connection once the request is done."
    return response


@app.route("/", methods=["GET"])
def listing():
    try:
        page = int(request.args.get("page", 1)) - 1
        limit = int(request.args.get("size", 10))
    except ValueError:
        return mk_error("Size and page must be integers")
    term = request.args.get("search", "%")
    if page < 0:
        return mk_error("Bad page number")
    if limit < 1:
        return mk_error("Bad page size")
    res = dbi.search(term, {"offset": (page * limit), "limit": limit})
    res = jsonify({"results": [{name: value} for name, value in res]})
    return res, 200


@app.route("/", methods=["POST"])
def update():
    payload = request.json
    if not payload:
        return mk_error("Missing JSON payload")
    updates = {"name": payload.get("name"), "value": payload.get("value")}
    if not all(v is not None for _, v in updates.items()):
        return mk_error("Missing name or value in payload")
    if not isinstance(updates["name"], str):
        return mk_error("Invalid value for name, must be string")
    if not isinstance(updates["value"], int):
        return mk_error("Invalid value for value, must be integer")
    updated = dbi.upsert(**updates)
    return jsonify({"updated": updated}), 200


@app.route("/summed", methods=["GET"])
def summed():
    term = request.args.get("search", "%")
    try:
        limit = int(request.args.get("size", 10))
    except ValueError:
        return mk_error("Size must be integer")
    return jsonify(dbi.sum_for(term, limit)), 200
