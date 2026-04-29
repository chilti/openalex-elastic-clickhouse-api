
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
CREATE TABLE IF NOT EXISTS rag.works_seed_mexico
ENGINE = MergeTree()
ORDER BY (publication_year, id)
AS 
SELECT * 
FROM rag.works
WHERE has(all_country_codes, 'MX')
   OR arrayExists(x -> x LIKE '%MEXICO%', institution_names)
"""

try:
    client = clickhouse_connect.get_client(host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASSWORD, database=CH_DB)
    # Comprobamos si ya existe para no repetir
    exists = client.query("SELECT count() FROM system.tables WHERE database = 'rag' AND name = 'works_seed_mexico'").result_rows[0][0]
    if exists:
        print("INFO: La tabla rag.works_seed_mexico ya existe. Saltando creacion.")
    else:
        print("STARTING: Iniciando materializacion de rag.works_seed_mexico...")
        client.command(query)
        print("SUCCESS: Tabla creada con exito.")
except Exception as e:
    print("ERROR: " + str(e).encode('ascii', 'ignore').decode('ascii'))
