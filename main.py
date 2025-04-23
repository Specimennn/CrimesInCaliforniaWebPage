# main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
from datetime import datetime
import requests

app = FastAPI()

# CORS para que tu frontend pueda hacer fetch
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def home():
    return {"status": "API activa"}

# Puedes agregar más rutas como esta:
@app.get("/fetch-data")
def fetch_data():
    # Aquí iría tu lógica de requests + PostgreSQL
    return {"msg": "Acá va tu lógica de fetch + insert"}
