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

# Criar tabela de companhias aéreas
cursor.execute("""
    CREATE TABLE airlines (
        airline_code    VARCHAR(10)     NOT NULL PRIMARY KEY,
        airline_name    VARCHAR(200)    NOT NULL,
        country         VARCHAR(100)    NOT NULL DEFAULT 'USA',
        alliance        VARCHAR(50)     NULL
    )
""")

print("Tabela airlines criada")

# As 18 companhias que existem no dataset
airlines = [
    ("WN", "Southwest Airlines",          "USA", None),
    ("DL", "Delta Air Lines",             "USA", "SkyTeam"),
    ("OO", "SkyWest Airlines",            "USA", None),
    ("AA", "American Airlines",           "USA", "Oneworld"),
    ("MQ", "American Eagle Airlines",     "USA", "Oneworld"),
    ("US", "US Airways",                  "USA", "Star Alliance"),
    ("XE", "JSX Air",                     "USA", None),
    ("EV", "ExpressJet Airlines",         "USA", None),
    ("UA", "United Airlines",             "USA", "Star Alliance"),
    ("CO", "Continental Airlines",        "USA", "Star Alliance"),
    ("FL", "AirTran Airways",             "USA", None),
    ("9E", "Endeavor Air",                "USA", "SkyTeam"),
    ("B6", "JetBlue Airways",             "USA", None),
    ("YV", "Mesa Airlines",               "USA", None),
    ("OH", "PSA Airlines",                "USA", "Oneworld"),
    ("AS", "Alaska Airlines",             "USA", "Oneworld"),
    ("F9", "Frontier Airlines",           "USA", None),
    ("HA", "Hawaiian Airlines",           "USA", None),
]

cursor.executemany(
    "INSERT INTO airlines VALUES (?,?,?,?)",
    airlines
)

print(f"{len(airlines)} companhias aéreas inseridas")

# Confirmar
cursor.execute("SELECT airline_code, airline_name, alliance FROM airlines ORDER BY airline_code")
rows = cursor.fetchall()
print("\n--- Companhias na base de dados ---")
for row in rows:
    alliance = row[2] if row[2] else "Independent"
    print(f"  {row[0]} | {row[1]:<30} | {alliance}")

conn.close()