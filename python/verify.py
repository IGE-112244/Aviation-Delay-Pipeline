import pyodbc
import os
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).parent.parent
load_dotenv(BASE_DIR / ".env")

conn = pyodbc.connect(os.getenv("AZURE_SQL_CONNECTION_STRING"), autocommit=True)
cursor = conn.cursor()
cursor.execute("SELECT COUNT(*) FROM fact_flights")
print(f"Linhas na fact_flights: {cursor.fetchone()[0]:,}")
conn.close()