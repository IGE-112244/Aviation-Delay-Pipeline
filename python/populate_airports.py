import pandas as pd
import pyodbc
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

conn = pyodbc.connect(
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=localhost,1433;"
    "UID=sa;"
    "PWD=Aviation2024!;"
    "TrustServerCertificate=yes;",
    autocommit=True
)

cursor = conn.cursor()
cursor.execute("USE aviation_ref")

# Apagar dados anteriores
cursor.execute("DELETE FROM airports")
print("Dados anteriores apagados")

# Carregar o ficheiro que descarregámos
df = pd.read_csv(os.path.join(BASE_DIR, "data/raw/airports_reference.csv"))

# Inserir linha a linha
inseridos = 0
for _, row in df.iterrows():
    try:
        cursor.execute(
            "INSERT INTO airports VALUES (?,?,?,?,?,?,?)",
            row["airport_code"],
            row["airport_name"],
            row["city"],
            row["state"],
            row["latitude"],
            row["longitude"],
            row["timezone"]
        )
        inseridos += 1
    except Exception as e:
        print(f"Erro a inserir {row['airport_code']}: {e}")

print(f"{inseridos} aeroportos inseridos com sucesso")

# Verificar cobertura dos aeroportos do CSV
df_voos = pd.read_csv(os.path.join(BASE_DIR, "data/raw/Airlines.csv"))
aeroportos_csv = set(
    df_voos["AirportFrom"].unique().tolist() +
    df_voos["AirportTo"].unique().tolist()
)

cursor.execute("SELECT airport_code FROM airports")
aeroportos_sql = set([row[0] for row in cursor.fetchall()])

cobertos = aeroportos_csv & aeroportos_sql
print(f"\nCobertura final: {len(cobertos)}/{len(aeroportos_csv)} aeroportos")
print(f"Percentagem: {len(cobertos)/len(aeroportos_csv)*100:.1f}%")

conn.close()