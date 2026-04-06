import pyodbc

# Ligar ao SQL Server
conn = pyodbc.connect(
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=localhost,1433;"
    "UID=sa;"
    "PWD=Aviation2024!;"
    "TrustServerCertificate=yes;",
    autocommit=True
)

cursor = conn.cursor()

# Criar a base de dados
cursor.execute("CREATE DATABASE aviation_ref")
conn.commit()

print("Base de dados aviation_ref criada com sucesso")

conn.close()