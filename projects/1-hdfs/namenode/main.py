from fastapi import FastAPI
import json

app = FastAPI()

with open("config.json") as f:
    config = json.load(f)

@app.get("/datanodes")
def get_datanodes():
    """
    Endpoint para devolver la lista de datanodes configurados.
    """
    return {"datanodes": config["datanodes"]}