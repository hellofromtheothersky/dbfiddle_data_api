from fastapi import FastAPI
from fastapi.encoders import jsonable_encoder
import pandas as pd
from gen_data import gen_data
from gen_schema import gen_schema

app = FastAPI()


@app.post("/gen_data")
async def generate_data(json_data: dict) -> dict:
    result = gen_data(json_data, list_output=True)
    return jsonable_encoder(result)


@app.post("/gen_schema")
async def generate_schema(json_data: dict) -> str:
    return gen_schema(json_data['query'])
