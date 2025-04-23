from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

# Para permitir solicitudes desde cualquier origen (o tu GitHub Pages)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Cambia esto a tu dominio si querés más seguridad
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DATABASE_URL = os.getenv("DATABASE_URL")

@app.get("/all-crimes")
def get_all_crime_data():
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()

    query = """
        SELECT date_rptd, date_occ, vict_age, vict_sex, vict_descent,
               area, weapon_used_cd, weapon_desc, crm_cd_desc,
               location, lat, lon
        FROM crime_data
    """

    cursor.execute(query)
    rows = cursor.fetchall()

    # Column names to dictionary keys
    columns = [desc[0] for desc in cursor.description]
    data = [dict(zip(columns, row)) for row in rows]

    cursor.close()
    conn.close()

    return data
