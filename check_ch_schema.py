
import clickhouse_connect
import os
from dotenv import load_dotenv

# Load .env from the other directory since I know where it is
load_dotenv(r'c:\Users\jlja\Documents\Proyectos\RAGs\.env')

CH_HOST = os.getenv("CH_HOST", "127.0.0.1")
CH_PORT = int(os.getenv("CH_PORT", 8123))
CH_USER = os.getenv("CH_USER", "admin")
CH_PASSWORD = os.getenv("CH_PASSWORD", "admin")
CH_DB       = os.getenv("CH_DATABASE", "rag")

print(f"Connecting to {CH_HOST}:{CH_PORT} as {CH_USER}...")

try:
    client = clickhouse_connect.get_client(host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASSWORD, database=CH_DB)
    
    print("\n--- Table: authors ---")
    res = client.query("SHOW CREATE TABLE authors")
    print(res.result_rows[0][0])
    
    print("\n--- Table Statistics ---")
    res = client.query("SELECT count() FROM authors")
    print(f"Total authors: {res.result_rows[0][0]}")
    
except Exception as e:
    print(f"Error: {e}")
