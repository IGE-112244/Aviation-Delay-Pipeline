import pyodbc
import pandas as pd
import os
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).parent.parent
load_dotenv(BASE_DIR / ".env")

CONNECTION_STRING = os.getenv("AZURE_SQL_CONNECTION_STRING")
CURATED = BASE_DIR / "data" / "curated"

print("=" * 60)
print("AVIATION DW — CARREGAR DADOS")
print("=" * 60)

conn = pyodbc.connect(CONNECTION_STRING, autocommit=True)
cursor = conn.cursor()


def load_table(nome: str, df: pd.DataFrame):
    """Carrega um DataFrame para uma tabela SQL."""
    print(f"\n── A carregar {nome}...")

    cursor.execute(f"DELETE FROM {nome}")

    # Converter coluna a coluna para garantir tipos correctos
    for col in df.columns:
        if df[col].dtype == "bool":
            df[col] = df[col].astype(int)

    # Converter DataFrame para lista de tuplos
    # substituindo qualquer NaN/NA por None
    rows = []
    for row in df.itertuples(index=False):
        tuplo = tuple(
            None if pd.isna(v) else v
            for v in row
        )
        rows.append(tuplo)

    cols = ", ".join(df.columns)
    placeholders = ", ".join(["?" for _ in df.columns])
    sql = f"INSERT INTO {nome} ({cols}) VALUES ({placeholders})"

    batch_size = 1000
    total = len(rows)
    inseridos = 0

    for i in range(0, total, batch_size):
        batch = rows[i:i + batch_size]
        cursor.executemany(sql, batch)
        inseridos += len(batch)
        if total > batch_size:
            pct = inseridos / total * 100
            print(f"  {inseridos:,}/{total:,} ({pct:.0f}%)", end="\r")

    print(f"  {nome}: {inseridos:,} linhas carregadas")


# Carregar dimensões primeiro
print("\n── Fase 1: Dimensões ───────────────────────────────────")
load_table("dim_airline", pd.read_parquet(CURATED / "dim_airline.parquet"))
load_table("dim_airport", pd.read_parquet(CURATED / "dim_airport.parquet"))
load_table("dim_day", pd.read_parquet(CURATED / "dim_day.parquet"))
load_table("dim_time", pd.read_parquet(CURATED / "dim_time.parquet"))
load_table("dim_weather", pd.read_parquet(CURATED / "dim_weather.parquet"))

# Carregar fact table
print("\n── Fase 2: Fact table ──────────────────────────────────")
df_fact = pd.read_parquet(CURATED / "fact_flights.parquet")

# Substituir NaN por None para SQL aceitar NULLs
df_fact = df_fact.where(pd.notnull(df_fact), None)

load_table("fact_flights", df_fact)

# Verificar contagens
print("\n── Verificação final ───────────────────────────────────")
tabelas = ["dim_airline", "dim_airport", "dim_day", "dim_time", "dim_weather", "fact_flights"]
for tabela in tabelas:
    cursor.execute(f"SELECT COUNT(*) FROM {tabela}")
    count = cursor.fetchone()[0]
    print(f"  {tabela:<25} {count:>10,} linhas")

conn.close()
print("\nCarga concluída com sucesso!")