from fastapi import FastAPI
from pydantic import BaseModel
import uvicorn

app = FastAPI()

users = [{"id": i + 1, "name": f"User {i + 1}"} for i in range(100)]


class EchoBody(BaseModel):
    pass


@app.get("/plaintext")
async def plaintext():
    return "Hello, World!"


@app.get("/json")
async def json_endpoint():
    return {"message": "Hello, World!"}


@app.get("/users/{user_id}")
async def get_user(user_id: str):
    return {"id": 0, "name": f"User {user_id}"}


@app.get("/search")
async def search(q: str = ""):
    return {"query": q}


@app.post("/echo")
async def echo(body: dict):
    return body


@app.get("/users")
async def list_users():
    return users


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=3101, log_level="warning")
