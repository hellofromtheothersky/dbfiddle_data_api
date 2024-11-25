from fastapi import FastAPI
import pandas as pd
from gen_data import gen_data

app = FastAPI()


@app.post("/gen_data")
async def generate_data(json_data: dict) -> dict:
    return gen_data(json_data, list_output=True)
