import json
import random
import os
from datetime import datetime, timedelta

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Aeroportos principais para simular sensores
AIRPORTS = [
    "ATL", "LAX", "ORD", "DFW", "DEN",
    "JFK", "SFO", "SEA", "LAS", "MCO",
    "IAH", "PHX", "BOS", "MSP", "DTW"
]

# Condições meteorológicas possíveis
CONDITIONS = ["Clear", "Cloudy", "Rain", "Snow", "Fog", "Storm"]
CONDITION_WEIGHTS = [40, 25, 15, 8, 7, 5]

def generate_reading(airport: str, timestamp: datetime) -> dict:
    """
    Gera uma leitura simulada de sensores para um aeroporto.
    Simula o formato JSON que chegaria via MQTT em produção.
    """
    condition = random.choices(CONDITIONS, weights=CONDITION_WEIGHTS)[0]

    # Temperatura varia com a condição meteorológica
    temp_ranges = {
        "Clear":  (15, 35),
        "Cloudy": (10, 25),
        "Rain":   (8, 20),
        "Snow":   (-10, 2),
        "Fog":    (5, 15),
        "Storm":  (5, 18),
    }
    temp_min, temp_max = temp_ranges[condition]

    # Taxa de ocupação dos gates varia ao longo do dia
    hour = timestamp.hour
    if 6 <= hour <= 9 or 16 <= hour <= 19:
        gate_occupancy = random.uniform(0.75, 0.98)  # Horas de pico
    elif 10 <= hour <= 15:
        gate_occupancy = random.uniform(0.50, 0.80)  # Horas normais
    else:
        gate_occupancy = random.uniform(0.10, 0.45)  # Horas baixas

    return {
        "sensor_id":        f"SENSOR_{airport}_{timestamp.strftime('%Y%m%d%H%M')}",
        "airport_code":     airport,
        "timestamp":        timestamp.isoformat(),
        "temperature_c":    round(random.uniform(temp_min, temp_max), 1),
        "wind_speed_kmh":   round(random.uniform(0, 80), 1),
        "visibility_km":    round(random.uniform(0.5, 15), 1),
        "weather_condition": condition,
        "gate_occupancy_pct": round(gate_occupancy * 100, 1),
        "active_runways":   random.randint(1, 4),
        "ground_delay_min": random.randint(0, 45) if condition in ["Storm", "Snow", "Fog"] else random.randint(0, 10),
    }

def generate_dataset(days: int = 7, interval_hours: int = 1) -> list:
    """
    Gera leituras de sensores para os últimos N dias
    com intervalo de X horas entre cada leitura.
    """
    readings = []
    start = datetime.now() - timedelta(days=days)
    current = start

    while current <= datetime.now():
        for airport in AIRPORTS:
            readings.append(generate_reading(airport, current))
        current += timedelta(hours=interval_hours)

    return readings

# Gerar o dataset
print("A gerar leituras de sensores...")
readings = generate_dataset(days=7, interval_hours=1)

# Guardar como JSON
output_path = os.path.join(BASE_DIR, "data/raw/sensor_readings.json")
with open(output_path, "w") as f:
    json.dump(readings, f, indent=2)

print(f"Geradas {len(readings):,} leituras")
print(f"Aeroportos: {len(AIRPORTS)}")
print(f"Período: últimos 7 dias, 1 leitura por hora")
print(f"Guardado em: {output_path}")

# Mostrar exemplo de uma leitura
print("\n--- Exemplo de leitura ---")
print(json.dumps(readings[0], indent=2))