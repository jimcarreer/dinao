from flask import Flask, request, jsonify

import dbi

app = Flask('dinao_flask_example')


def mk_error(error: str, status: int = 400):
    return jsonify({"error": error}), status


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
