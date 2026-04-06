import pandas as pd
import urllib.request
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

URL = "https://davidmegginson.github.io/ourairports-data/airports.csv"
RAW = os.path.join(BASE_DIR, "data/raw/airports_reference_raw.csv")
DESTINO = os.path.join(BASE_DIR, "data/raw/airports_reference.csv")

print("A descarregar dados de aeroportos...")
urllib.request.urlretrieve(URL, RAW)

df = pd.read_csv(RAW)

df_us = df[
    (df["iso_country"] == "US") &
    (df["iata_code"].notna()) &
    (df["iata_code"] != "") &
    (df["type"].isin(["large_airport", "medium_airport"]))
].copy()

df_us = df_us[[
    "iata_code",
    "name",
    "municipality",
    "iso_region",
    "latitude_deg",
    "longitude_deg"
]].rename(columns={
    "iata_code":     "airport_code",
    "name":          "airport_name",
    "municipality":  "city",
    "iso_region":    "state",
    "latitude_deg":  "latitude",
    "longitude_deg": "longitude"
})

df_us["state"] = df_us["state"].str.replace("US-", "")
df_us["timezone"] = "America/New_York"

# Guardar o ficheiro JÁ com os nomes correctos
df_us.to_csv(DESTINO, index=False)
print(f"Ficheiro guardado com {len(df_us)} aeroportos")
print(f"Colunas: {df_us.columns.tolist()}")