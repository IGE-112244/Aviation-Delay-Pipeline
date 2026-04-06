import os
import sys

# Garante que o caminho base é sempre a raiz do projecto
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)

# Agora usa o caminho relativo à raiz
df = pd.read_csv(os.path.join(BASE_DIR, "data/raw/Airlines.csv"))

# Todos os aeroportos únicos no CSV (origem + destino)
aeroportos_csv = set(
    df["AirportFrom"].unique().tolist() +
    df["AirportTo"].unique().tolist()
)

print(f"Total de aeroportos únicos no CSV: {len(aeroportos_csv)}")

# Ligar ao SQL Server e buscar os aeroportos que temos
conn = pyodbc.connect(
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=localhost,1433;"
    "UID=sa;"
    "PWD=Aviation2024!;"
    "TrustServerCertificate=yes;"
)

cursor = conn.cursor()
cursor.execute("USE aviation_ref")
cursor.execute("SELECT airport_code FROM airports")
aeroportos_sql = set([row[0] for row in cursor.fetchall()])
conn.close()

# Cobertura
cobertos = aeroportos_csv & aeroportos_sql
nao_cobertos = aeroportos_csv - aeroportos_sql

print(f"Aeroportos cobertos pelo SQL Server: {len(cobertos)}")
print(f"Aeroportos não cobertos: {len(nao_cobertos)}")
print(f"Cobertura: {len(cobertos)/len(aeroportos_csv)*100:.1f}%")

print(f"\nAeroportos em falta:")
print(sorted(nao_cobertos))