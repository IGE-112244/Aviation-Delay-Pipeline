import os
from pathlib import Path
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient

# Carregar variáveis de ambiente
BASE_DIR = Path(__file__).parent.parent
load_dotenv(BASE_DIR / ".env")

CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER_CURATED = os.getenv("AZURE_CONTAINER_CURATED")
CONTAINER_RAW = os.getenv("AZURE_CONTAINER_RAW")

print("=" * 60)
print("AVIATION PIPELINE — UPLOAD PARA AZURE DATA LAKE")
print("=" * 60)

# Criar cliente
client = BlobServiceClient.from_connection_string(CONNECTION_STRING)

def upload_file(container: str, local_path: Path, blob_name: str):
    """Faz upload de um ficheiro para o Azure Data Lake."""
    try:
        container_client = client.get_container_client(container)
        with open(local_path, "rb") as data:
            container_client.upload_blob(
                name=blob_name,
                data=data,
                overwrite=True
            )
        size_kb = local_path.stat().st_size / 1024
        print(f"  OK  {blob_name:<40} {size_kb:>8.1f} KB")
        return True
    except Exception as e:
        print(f"  ERRO {blob_name}: {e}")
        return False

# ── Upload ficheiros Parquet (curated) ────────────────────────
print("\n── Upload para container curated ───────────────────────")
curated_path = BASE_DIR / "data" / "curated"
parquet_files = list(curated_path.glob("*.parquet"))

sucesso = 0
for local_file in parquet_files:
    blob_name = f"aviation/{local_file.name}"
    if upload_file(CONTAINER_CURATED, local_file, blob_name):
        sucesso += 1

print(f"\n{sucesso}/{len(parquet_files)} ficheiros carregados para curated")

# ── Upload ficheiros Raw ───────────────────────────────────────
print("\n── Upload para container raw ───────────────────────────")
raw_files = [
    BASE_DIR / "data" / "raw" / "Airlines.csv",
    BASE_DIR / "data" / "raw" / "sensor_readings.json",
    BASE_DIR / "data" / "raw" / "airports_reference.csv",
]

sucesso_raw = 0
for local_file in raw_files:
    if local_file.exists():
        blob_name = f"aviation/{local_file.name}"
        if upload_file(CONTAINER_RAW, local_file, blob_name):
            sucesso_raw += 1

print(f"\n{sucesso_raw}/{len(raw_files)} ficheiros carregados para raw")
print("\nUpload concluído!")