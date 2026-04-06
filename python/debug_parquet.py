import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
CURATED = BASE_DIR / "data" / "curated"

df = pd.read_parquet(CURATED / "dim_airline.parquet")
print("Tipos de dados:")
print(df.dtypes)
print("\nPrimeiras linhas:")
print(df.head())
print("\nNulls:")
print(df.isnull().sum())