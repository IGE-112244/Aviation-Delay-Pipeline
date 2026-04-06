import pandas as pd
import json
import pyodbc
import os
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

print("=" * 60)
print("AVIATION PIPELINE — TRANSFORMAÇÃO")
print("=" * 60)

# ── 1. CARREGAR AS TRÊS FONTES ────────────────────────────────

print("\n── Fase 1: Carregar fontes ─────────────────────────────")

# Fonte 1: CSV
df_flights = pd.read_csv(os.path.join(BASE_DIR, "data/raw/Airlines.csv"))
print(f"CSV carregado: {len(df_flights):,} voos")

# Fonte 2: SQL Server
conn = pyodbc.connect(
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=localhost,1433;"
    "DATABASE=aviation_ref;"
    "UID=sa;"
    "PWD=Aviation2024!;"
    "TrustServerCertificate=yes;"
)
cursor = conn.cursor()

# Aeroportos
cursor.execute("SELECT airport_code, airport_name, city, state, latitude, longitude FROM airports")
rows = cursor.fetchall()
df_airports = pd.DataFrame.from_records(
    rows,
    columns=["airport_code", "airport_name", "city", "state", "latitude", "longitude"]
)
print(f"SQL Server carregado: {len(df_airports):,} aeroportos")

# Companhias aéreas
cursor.execute("SELECT airline_code, airline_name, country, alliance FROM airlines")
rows = cursor.fetchall()
df_airlines = pd.DataFrame.from_records(
    rows,
    columns=["airline_code", "airline_name", "country", "alliance"]
)
print(f"SQL Server carregado: {len(df_airlines):,} companhias aéreas")

conn.close()

# Fonte 3: JSON sensores
with open(os.path.join(BASE_DIR, "data/raw/sensor_readings.json"), "r") as f:
    sensor_data = json.load(f)
df_sensors = pd.DataFrame(sensor_data)
print(f"JSON carregado: {len(df_sensors):,} leituras de sensores")

# ── 2. LIMPEZA ────────────────────────────────────────────────

print("\n── Fase 2: Limpeza ─────────────────────────────────────")

# Remover duplicados
antes = len(df_flights)
df_flights = df_flights.drop_duplicates()
print(f"Duplicados removidos: {antes - len(df_flights)}")

# Remover voos com duração zero
antes = len(df_flights)
df_flights = df_flights[df_flights["Length"] > 0]
print(f"Voos com duração zero removidos: {antes - len(df_flights)}")

# Converter coluna Delay para booleano
df_flights["is_delayed"] = df_flights["Delay"].astype(bool)
print("Coluna Delay convertida para booleano")

# ── 3. CONSTRUIR AS DIMENSION TABLES ─────────────────────────

print("\n── Fase 3: Construir dimensões ─────────────────────────")

# dim_airline
dim_airline = df_airlines.copy()
dim_airline.insert(0, "airline_id", range(1, len(dim_airline) + 1))
print(f"dim_airline: {len(dim_airline)} linhas")

# dim_airport
dim_airport = df_airports.copy()
dim_airport.insert(0, "airport_id", range(1, len(dim_airport) + 1))
print(f"dim_airport: {len(dim_airport)} linhas")

# dim_day
dim_day = pd.DataFrame({
    "day_id":     [1, 2, 3, 4, 5, 6, 7],
    "day_number": [1, 2, 3, 4, 5, 6, 7],
    "day_name":   ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"],
    "day_short":  ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"],
    "is_weekend": [False, False, False, False, False, True, True],
})
print(f"dim_day: {len(dim_day)} linhas")

# dim_time — uma linha por minuto seria demasiado
# Usamos granularidade de hora (0 a 23)
dim_time_rows = []
for hour in range(24):
    minutes = hour * 60
    if 5 <= hour <= 11:
        period = "Morning"
    elif 12 <= hour <= 17:
        period = "Afternoon"
    elif 18 <= hour <= 21:
        period = "Evening"
    else:
        period = "Night"

    dim_time_rows.append({
        "time_id":               hour + 1,
        "hour":                  hour,
        "time_label":            f"{hour:02d}:00",
        "period":                period,
        "is_peak_hour":          hour in [7, 8, 9, 17, 18, 19],
    })

dim_time = pd.DataFrame(dim_time_rows)
print(f"dim_time: {len(dim_time)} linhas (granularidade horária)")

# dim_weather — agregar sensores por aeroporto e hora
df_sensors["timestamp"] = pd.to_datetime(df_sensors["timestamp"])
df_sensors["hour"] = df_sensors["timestamp"].dt.hour

dim_weather = df_sensors.groupby(["airport_code", "hour"]).agg(
    temperature_c=("temperature_c", "mean"),
    wind_speed_kmh=("wind_speed_kmh", "mean"),
    visibility_km=("visibility_km", "mean"),
    weather_condition=("weather_condition", lambda x: x.mode()[0]),
    gate_occupancy_pct=("gate_occupancy_pct", "mean"),
    ground_delay_min=("ground_delay_min", "mean"),
).reset_index()

dim_weather.insert(0, "weather_id", range(1, len(dim_weather) + 1))
dim_weather["temperature_c"] = dim_weather["temperature_c"].round(1)
dim_weather["wind_speed_kmh"] = dim_weather["wind_speed_kmh"].round(1)
dim_weather["visibility_km"] = dim_weather["visibility_km"].round(1)
dim_weather["gate_occupancy_pct"] = dim_weather["gate_occupancy_pct"].round(1)
dim_weather["ground_delay_min"] = dim_weather["ground_delay_min"].round(1)
print(f"dim_weather: {len(dim_weather)} linhas")

# ── 4. CONSTRUIR A FACT TABLE ─────────────────────────────────

print("\n── Fase 4: Construir fact table ────────────────────────")

# Criar mapeamentos de código para ID
airline_map = dict(zip(dim_airline["airline_code"], dim_airline["airline_id"]))
airport_map = dict(zip(dim_airport["airport_code"], dim_airport["airport_id"]))
weather_map = dict(zip(
    zip(dim_weather["airport_code"], dim_weather["hour"]),
    dim_weather["weather_id"]
))

# Calcular hora a partir dos minutos
df_flights["hour"] = (df_flights["Time"] // 60).astype(int).clip(0, 23)

# Calcular time_id
df_flights["time_id"] = df_flights["hour"] + 1

# Construir a fact table
fact_flights = pd.DataFrame({
    "flight_id":          df_flights["id"],
    "airline_id":         df_flights["Airline"].map(airline_map),
    "origin_airport_id":  df_flights["AirportFrom"].map(airport_map),
    "dest_airport_id":    df_flights["AirportTo"].map(airport_map),
    "day_id":             df_flights["DayOfWeek"],
    "time_id":            df_flights["time_id"],
    "weather_id":         df_flights.apply(
        lambda r: weather_map.get((r["AirportFrom"], r["hour"])), axis=1
    ),
    "flight_number":      df_flights["Flight"],
    "scheduled_time_min": df_flights["Time"],
    "flight_duration_min":df_flights["Length"],
    "is_delayed":         df_flights["is_delayed"],
})

print(f"fact_flights: {len(fact_flights):,} linhas")
print(f"Nulls em airline_id: {fact_flights['airline_id'].isnull().sum()}")
print(f"Nulls em origin_airport_id: {fact_flights['origin_airport_id'].isnull().sum()}")
print(f"Nulls em dest_airport_id: {fact_flights['dest_airport_id'].isnull().sum()}")
print(f"Nulls em weather_id: {fact_flights['weather_id'].isnull().sum()}")

# ── 5. GUARDAR EM PARQUET ─────────────────────────────────────

print("\n── Fase 5: Guardar em Parquet ──────────────────────────")

CURATED = os.path.join(BASE_DIR, "data/curated")
os.makedirs(CURATED, exist_ok=True)

tabelas = {
    "dim_airline":  dim_airline,
    "dim_airport":  dim_airport,
    "dim_day":      dim_day,
    "dim_time":     dim_time,
    "dim_weather":  dim_weather,
    "fact_flights": fact_flights,
}

for nome, df in tabelas.items():
    path = os.path.join(CURATED, f"{nome}.parquet")
    df.to_parquet(path, index=False)
    size_kb = os.path.getsize(path) / 1024
    print(f"  {nome:<20} {len(df):>8,} linhas  {size_kb:>8.1f} KB")

print("\nTransformação concluída com sucesso!")