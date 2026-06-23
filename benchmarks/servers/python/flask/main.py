from flask import Flask, request, jsonify

app = Flask(__name__)

users = [{"id": i + 1, "name": f"User {i + 1}"} for i in range(100)]


@app.route("/plaintext")
def plaintext():
    return "Hello, World!"


@app.route("/json")
def json_endpoint():
    return jsonify({"message": "Hello, World!"})


@app.route("/users/<user_id>")
def get_user(user_id):
    return jsonify({"id": 0, "name": f"User {user_id}"})


@app.route("/search")
def search():
    q = request.args.get("q", "")
    return jsonify({"query": q})


@app.route("/echo", methods=["POST"])
def echo():
    return jsonify(request.get_json())


@app.route("/users")
def list_users():
    return jsonify(users)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=3102, debug=False)
