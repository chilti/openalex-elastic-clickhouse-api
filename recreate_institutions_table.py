
import clickhouse_connect
import os
from dotenv import load_dotenv

load_dotenv(r'c:\Users\jlja\Documents\Proyectos\RAGs\.env')

CH_HOST = os.getenv("CH_HOST")
CH_PORT = int(os.getenv("CH_PORT", 8124))
CH_USER = os.getenv("CH_USER")
CH_PASSWORD = os.getenv("CH_PASSWORD")
CH_DB = os.getenv("CH_DATABASE")

query = """
CREATE TABLE IF NOT EXISTS rag.institutions_seed_mexico
ENGINE = MergeTree()
ORDER BY (display_name, id)
AS
SELECT 
    id,
    display_name,
    ror,
    type,
    country_code,
    JSONExtractString(raw_data, 'geo', 'city') as city,
    JSONExtractString(raw_data, 'geo', 'region') as state,
    # Extraemos acronimos
    JSONExtract(raw_data, 'display_name_acronyms', 'Array(String)') as acronyms,
    # Jerarquia
    arrayFilter(x -> x.6 = 'parent', 
        JSONExtract(raw_data, 'associated_institutions', 'Array(Tuple(id String, ror String, display_name String, country_code String, type String, relationship String))')
    ) as parents,
    if(empty(parents), '', parents[1].1) as parent_id,
    if(empty(parents), '', parents[1].3) as parent_name,
    raw_data
FROM rag.institutions
WHERE country_code = 'MX'
   OR lower(display_name) LIKE '%mexico%'
   OR ror != '';
"""

try:
    client = clickhouse_connect.get_client(host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASSWORD, database=CH_DB)
    client.command("DROP TABLE IF EXISTS rag.institutions_seed_mexico")
    print("STARTING: Recreando tabla rag.institutions_seed_mexico...")
    client.command(query)
    print("SUCCESS: Tabla recreada con exito.")
except Exception as e:
    print("ERROR: " + str(e).encode('ascii', 'ignore').decode('ascii'))
