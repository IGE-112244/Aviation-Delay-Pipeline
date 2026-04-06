import os
from pathlib import Path
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient

BASE_DIR = Path(__file__).parent.parent
load_dotenv(BASE_DIR / ".env")

CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

# Timeout aumentado para ficheiros grandes
client = BlobServiceClient.from_connection_string(
    CONNECTION_STRING,
    connection_timeout=300,
    read_timeout=300
)

local_file = BASE_DIR / "data" / "raw" / "Airlines.csv"
container_client = client.get_container_client("raw")

print("A fazer upload do Airlines.csv...")
print("Pode demorar 2-3 minutos...")

with open(local_file, "rb") as data:
    container_client.upload_blob(
        name="aviation/Airlines.csv",
        data=data,
        overwrite=True,
        max_concurrency=4
    )

size_mb = local_file.stat().st_size / 1024 / 1024
print(f"Upload concluído: {size_mb:.1f} MB")