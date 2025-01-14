from typing import Union
from fastapi import FastAPI

app = FastAPI()

@app.get("/")
def read_root():
    return {"Hello": "World"}

@app.get("/items/{item_id}")
def read_item(item_id: int, q: Union[str, None] = None):
    return {"item_id": item_id, "q": q}


@app.post("/sum")
def calculate_sum(payload: dict):
    x = payload.get("x")
    y = payload.get("y")
    if x is None or y is None:
        return {"error": "x and y are required"}
    return {"result": x + y}
