import os
from dotenv import load_dotenv
from pathlib import Path

env_path = Path(os.getcwd()) / ".env"
print(f"Loading .env from: {env_path}")
load_dotenv(dotenv_path=env_path)

DB_URL = f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}" \
         f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"

SOURCE_API_BASE = os.getenv("SOURCE_API_BASE")
SOURCE_API_ENDPOINT = os.getenv("SOURCE_API_ENDPOINT")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))

print("BASE =", SOURCE_API_BASE)
print("ENDPOINT =", SOURCE_API_ENDPOINT)
      
