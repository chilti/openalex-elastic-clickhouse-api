import clickhouse_connect
import os
import sys
import logging
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Cargar variables de entorno desde ruta relativa (Linux)
load_dotenv('clickhouse_api/.env')

CH_HOST = os.getenv("CH_HOST", "localhost")
CH_PORT = int(os.getenv("CH_PORT", 8124))
CH_USER = os.getenv("CH_USER", "default")
CH_PASSWORD = os.getenv("CH_PASSWORD", "")
CH_DB = os.getenv("CH_DATABASE", "rag")

REBUILD = '--rebuild' in sys.argv

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

def main():
    try:
        client = clickhouse_connect.get_client(
            host=CH_HOST, 
            port=CH_PORT, 
            username=CH_USER, 
            password=CH_PASSWORD, 
            database=CH_DB,
            send_receive_timeout=1200 # Timeout extendido para carga masiva
        )
        
        if REBUILD:
            logger.info("⚠️ Flag --rebuild detectado. Eliminando tabla anterior...")
            client.command("DROP TABLE IF EXISTS rag.works_seed_mexico")
            
        # Comprobamos si ya existe para no repetir
        exists = client.query("SELECT count() FROM system.tables WHERE database = 'rag' AND name = 'works_seed_mexico'").result_rows[0][0]
        if exists:
            logger.info("La tabla rag.works_seed_mexico ya existe. Usa --rebuild para recrearla.")
        else:
            logger.info("Iniciando materializacion de rag.works_seed_mexico desde works...")
            client.command(query)
            
            # Validar resultado
            count = client.query("SELECT count() FROM rag.works_seed_mexico").first_row[0]
            logger.info(f"✅ Tabla works_seed_mexico creada con éxito. Total de registros: {count:,}")
            
    except Exception as e:
        logger.error(f"Error en populate_works_seed: {e}")

if __name__ == '__main__':
    main()
