import pyodbc
import pandas as pd
import os
from pathlib import Path
from dotenv import load_dotenv
import time

BASE_DIR = Path(__file__).parent.parent
load_dotenv(BASE_DIR / ".env")

CONNECTION_STRING = os.getenv("AZURE_SQL_CONNECTION_STRING")
CURATED = BASE_DIR / "data" / "curated"

def get_connection():
    conn = pyodbc.connect(CONNECTION_STRING, autocommit=True)
    conn.cursor().fast_executemany = True
    return conn

def get_max_id(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT MAX(flight_id) FROM fact_flights")
    result = cursor.fetchone()[0]
    return result if result else 0

# Carregar o Parquet completo
df_fact = pd.read_parquet(CURATED / "fact_flights.parquet")
df_fact["is_delayed"] = df_fact["is_delayed"].astype(int)

cols = ", ".join(df_fact.columns)
placeholders = ", ".join(["?" for _ in df_fact.columns])
sql = f"INSERT INTO fact_flights ({cols}) VALUES ({placeholders})"

total_geral = len(df_fact)
batch_size = 2000

while True:
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.fast_executemany = True

        max_id = get_max_id(conn)
        df_remaining = df_fact[df_fact["flight_id"] > max_id].copy()

        if len(df_remaining) == 0:
            print(f"\nTodos os dados inseridos!")
            cursor.execute("SELECT COUNT(*) FROM fact_flights")
            print(f"Total final: {cursor.fetchone()[0]:,} linhas")
            conn.close()
            break

        print(f"A continuar a partir do flight_id {max_id:,} — faltam {len(df_remaining):,} linhas")

        rows = []
        for row in df_remaining.itertuples(index=False):
            tuplo = tuple(None if pd.isna(v) else v for v in row)
            rows.append(tuplo)

        for i in range(0, len(rows), batch_size):
            batch = rows[i:i+batch_size]
            cursor.executemany(sql, batch)
            inseridos_total = max_id + i + len(batch)
            pct = inseridos_total / total_geral * 100
            print(f"  {inseridos_total:,}/{total_geral:,} ({pct:.0f}%)")

        conn.close()

    except Exception as e:
        print(f"\nLigação perdida: {e}")
        print("A reconectar em 5 segundos...")
        time.sleep(5)
        continue