
import clickhouse_connect
import os
from dotenv import load_dotenv

# Load config
load_dotenv(r'c:\Users\jlja\Documents\Proyectos\RAGs\.env')

CH_HOST = os.getenv("CH_HOST")
CH_PORT = int(os.getenv("CH_PORT", 8124))
CH_USER = os.getenv("CH_USER")
CH_PASSWORD = os.getenv("CH_PASSWORD")
CH_DB = os.getenv("CH_DATABASE")

insert_sql = """
INSERT INTO rag.authors_seed_mexico 
SELECT 
    id, 
    display_name, 
    orcid, 
    ids, 
    raw_data 
FROM rag.authors 
WHERE lower(raw_data) LIKE '%"country_code":"mx"%' 
   OR lower(raw_data) LIKE '%"country_code": "mx"%'
   OR lower(display_name) LIKE '%unam%' 
   OR lower(display_name) LIKE '%ipn%'
   OR lower(raw_data) LIKE '%"display_name":"mexico"%'
"""

try:
    client = clickhouse_connect.get_client(host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASSWORD, database=CH_DB)
    print("STARTING: Insercion corregida en rag.authors_seed_mexico...")
    client.command(insert_sql)
    print("SUCCESS: Insercion completada con exito.")
except Exception as e:
    print("ERROR durante la insercion: " + str(e).encode('ascii', 'ignore').decode('ascii'))
