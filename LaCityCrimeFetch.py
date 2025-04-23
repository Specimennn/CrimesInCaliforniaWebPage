import requests
import psycopg2
from datetime import datetime
import time

# Database Connection Config
DB_HOST = "localhost"
DB_NAME = "lacity"
DB_USER = "postgres"
DB_PASSWORD = "123456789"
API_URL = "https://data.lacity.org/resource/2nrs-mtv8.json"
ROWS_TO_FETCH = 10000  # Total rows to fetch
BATCH_SIZE = 1000  # Number of rows per request

# Function to Fetch Data with Pagination
def fetch_data():
    all_data = []
    for offset in range(0, ROWS_TO_FETCH, BATCH_SIZE):
        print(f"Fetching records {offset + 1} to {offset + BATCH_SIZE}...")
        response = requests.get(f"{API_URL}?$limit={BATCH_SIZE}&$offset={offset}")
        if response.status_code == 200:
            batch_data = response.json()
            if not batch_data:  # Stop if no more data
                break
            all_data.extend(batch_data)
        else:
            print(f"Failed to fetch data at offset {offset}: {response.status_code}")
            break
        time.sleep(1)  # Avoid overwhelming the API
    return all_data

# Function to Convert Floating Timestamps to PostgreSQL TIMESTAMP Format
def parse_date(date_str):
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S.%f")  # Handles floating timestamp format
    except ValueError:
        try:
            return datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S")  # Handles cases without milliseconds
        except ValueError:
            return None  # Handles any other inconsistencies

# Function to Clean Victim Age (Convert "0" to NULL)
def clean_vict_age(vict_age):
    return None if vict_age in ["0", 0] else vict_age  # Convert "0" to None

# Function to Insert Data into PostgreSQL
def insert_data(data):
    conn = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    cursor = conn.cursor()

    insert_query = """
    INSERT INTO crime_data (date_rptd, date_occ, vict_age, vict_sex, vict_descent, area, weapon_used_cd, weapon_desc, crm_cd_desc, location, lat, lon)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    for row in data:
        try:
            cursor.execute(insert_query, (
                parse_date(row.get("date_rptd")),  # Convert to TIMESTAMP
                parse_date(row.get("date_occ")),  # Convert to TIMESTAMP
                clean_vict_age(row.get("vict_age")),  # Convert "0" to NULL
                row.get("vict_sex"),
                row.get("vict_descent"),
                row.get("area"),
                row.get("weapon_used_cd"),
                row.get("weapon_desc"),
                row.get("crm_cd_desc"),  # New Crime Code Description Column
                row.get("location"),
                float(row["lat"]) if row.get("lat") else None,
                float(row["lon"]) if row.get("lon") else None
            ))
        except Exception as e:
            print("Error inserting row:", e)

    conn.commit()
    cursor.close()
    conn.close()
    print("Data successfully inserted into PostgreSQL.")

# Main Execution
if __name__ == "__main__":
    data = fetch_data()
    if data:
        insert_data(data)