
import clickhouse_connect
import os
from dotenv import load_dotenv

# Load .env
load_dotenv(r'c:\Users\jlja\Documents\Proyectos\RAGs\.env')

CH_ORCID_HOST = os.getenv("CH_ORCID_HOST", "127.0.0.1")
CH_ORCID_PORT = int(os.getenv("CH_ORCID_PORT", 8123))
CH_ORCID_USER = os.getenv("CH_ORCID_USER", "default")
CH_ORCID_PASS = os.getenv("CH_ORCID_PASSWORD", "")
CH_DB_ORCID   = os.getenv("CH_ORCID_DATABASE", "orcid")

print(f"Connecting to LOCAL {CH_ORCID_HOST}:{CH_ORCID_PORT} as {CH_ORCID_USER}...")

try:
    client = clickhouse_connect.get_client(host=CH_ORCID_HOST, port=CH_ORCID_PORT, username=CH_ORCID_USER, password=CH_ORCID_PASS, database=CH_DB_ORCID)
    
    print("\n--- Table: orcid_records ---")
    res = client.query("SHOW CREATE TABLE orcid_records")
    print(res.result_rows[0][0])
    
    print("\n--- Table Statistics ---")
    res = client.query("SELECT count() FROM orcid_records")
    print(f"Total ORCID records: {res.result_rows[0][0]}")
    
except Exception as e:
    print(f"Error: {e}")
