import pyodbc
import os
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).parent.parent
load_dotenv(BASE_DIR / ".env")

CONNECTION_STRING = os.getenv("AZURE_SQL_CONNECTION_STRING")

print("=" * 60)
print("AVIATION DW — CRIAR STAR SCHEMA")
print("=" * 60)

conn = pyodbc.connect(CONNECTION_STRING, autocommit=True)
cursor = conn.cursor()

# ── Apagar tabelas se existirem (ordem importa por causa das FKs)
print("\n── A limpar tabelas existentes...")
tabelas = ["fact_flights", "dim_airline", "dim_airport", "dim_day", "dim_time", "dim_weather"]
for tabela in tabelas:
    cursor.execute(f"IF OBJECT_ID('{tabela}', 'U') IS NOT NULL DROP TABLE {tabela}")
print("Feito")

# ── dim_airline
print("\n── A criar dim_airline...")
cursor.execute("""
    CREATE TABLE dim_airline (
        airline_id      INT             NOT NULL PRIMARY KEY,
        airline_code    NVARCHAR(10)    NOT NULL,
        airline_name    NVARCHAR(200)   NOT NULL,
        country         NVARCHAR(100)   NULL,
        alliance        NVARCHAR(50)    NULL
    )
""")
print("dim_airline criada")

# ── dim_airport
print("── A criar dim_airport...")
cursor.execute("""
    CREATE TABLE dim_airport (
        airport_id      INT             NOT NULL PRIMARY KEY,
        airport_code    NVARCHAR(10)    NOT NULL,
        airport_name    NVARCHAR(200)   NOT NULL,
        city            NVARCHAR(100)   NULL,
        state           NVARCHAR(50)    NULL,
        latitude        DECIMAL(9,6)    NULL,
        longitude       DECIMAL(9,6)    NULL
    )
""")
print("dim_airport criada")

# ── dim_day
print("── A criar dim_day...")
cursor.execute("""
    CREATE TABLE dim_day (
        day_id          INT             NOT NULL PRIMARY KEY,
        day_number      INT             NOT NULL,
        day_name        NVARCHAR(20)    NOT NULL,
        day_short       NVARCHAR(5)     NOT NULL,
        is_weekend      BIT             NOT NULL
    )
""")
print("dim_day criada")

# ── dim_time
print("── A criar dim_time...")
cursor.execute("""
    CREATE TABLE dim_time (
        time_id         INT             NOT NULL PRIMARY KEY,
        hour            INT             NOT NULL,
        time_label      NVARCHAR(10)    NOT NULL,
        period          NVARCHAR(20)    NOT NULL,
        is_peak_hour    BIT             NOT NULL
    )
""")
print("dim_time criada")

# ── dim_weather
print("── A criar dim_weather...")
cursor.execute("""
    CREATE TABLE dim_weather (
        weather_id          INT             NOT NULL PRIMARY KEY,
        airport_code        NVARCHAR(10)    NOT NULL,
        hour                INT             NOT NULL,
        temperature_c       DECIMAL(5,1)    NULL,
        wind_speed_kmh      DECIMAL(5,1)    NULL,
        visibility_km       DECIMAL(5,1)    NULL,
        weather_condition   NVARCHAR(50)    NULL,
        gate_occupancy_pct  DECIMAL(5,1)    NULL,
        ground_delay_min    DECIMAL(5,1)    NULL
    )
""")
print("dim_weather criada")

# ── fact_flights
print("── A criar fact_flights...")
cursor.execute("""
    CREATE TABLE fact_flights (
        flight_id               BIGINT          NOT NULL PRIMARY KEY,
        airline_id              INT             NULL,
        origin_airport_id       INT             NULL,
        dest_airport_id         INT             NULL,
        day_id                  INT             NULL,
        time_id                 INT             NULL,
        weather_id              INT             NULL,
        flight_number           INT             NULL,
        scheduled_time_min      INT             NULL,
        flight_duration_min     INT             NULL,
        is_delayed              BIT             NOT NULL
    )
""")
print("fact_flights criada")

print("\n── Verificar tabelas criadas...")
cursor.execute("""
    SELECT TABLE_NAME
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_TYPE = 'BASE TABLE'
    ORDER BY TABLE_NAME
""")
tabelas_criadas = [row[0] for row in cursor.fetchall()]
print(f"Tabelas na base de dados: {tabelas_criadas}")

conn.close()
print("\nStar schema criado com sucesso!")