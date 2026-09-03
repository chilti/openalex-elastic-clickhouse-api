import clickhouse_connect
import os
import logging
import time
import argparse
from dotenv import load_dotenv

# Configuración de logs
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Cargar variables de entorno
load_dotenv('clickhouse_api/.env')

CH_HOST = os.environ.get('CH_HOST', 'localhost')
CH_PORT = int(os.environ.get('CH_PORT', 8124))
CH_USER = os.environ.get('CH_USER', 'default')
CH_PASSWORD = os.environ.get('CH_PASSWORD', '')
CH_DATABASE = os.environ.get('CH_DATABASE', 'rag')

PAUSE_BETWEEN_YEARS = 30  # Segundos de pausa

def get_client():
    return clickhouse_connect.get_client(
        host=CH_HOST,
        port=CH_PORT,
        username=CH_USER,
        password=CH_PASSWORD,
        database=CH_DATABASE,
        send_receive_timeout=1800  # 30 minutos para inserts masivos
    )

def init_schema(client):
    """Crea la tabla destino y la vista materializada."""
    
    logger.info("Creando tabla work_citations si no existe...")
    client.command("""
        CREATE TABLE IF NOT EXISTS rag.work_citations (
            cited_work_id String,
            citing_work_id String,
            citing_publication_year UInt16
        ) ENGINE = MergeTree()
        PARTITION BY citing_publication_year
        ORDER BY (cited_work_id, citing_publication_year)
    """)
    logger.info("✅ Tabla work_citations lista.")
    
    logger.info("Creando Materialized View work_citations_mv si no existe...")
    try:
        client.command("""
            CREATE MATERIALIZED VIEW IF NOT EXISTS rag.work_citations_mv
            TO rag.work_citations
            AS SELECT 
                arrayJoin(referenced_works) AS cited_work_id,
                id AS citing_work_id,
                publication_year AS citing_publication_year
            FROM rag.works_flat
        """)
        logger.info("✅ Vista work_citations_mv lista.")
    except Exception as e:
        logger.warning(f"No se pudo crear la vista (posiblemente esté en estado detached): {e}")

def run_migration(client, dry_run=False, limit_years=None):
    # 1. Escanear todos los años disponibles en works_flat que tengan referencias
    logger.info("Escaneando años disponibles en works_flat...")
    
    # Solo escaneamos años donde hay artículos con referenced_works no vacíos
    query_years = """
        SELECT publication_year, count() 
        FROM rag.works_flat 
        WHERE publication_year > 0 AND length(referenced_works) > 0
        GROUP BY publication_year 
        ORDER BY publication_year DESC
    """
    years_data = client.query(query_years).result_rows
    
    if limit_years:
        years_data = years_data[:limit_years]
        
    logger.info(f"Se procesarán {len(years_data)} años distintos.")
    
    for row in years_data:
        year = int(row[0])
        total_works = row[1]
        
        if dry_run:
            logger.info(f"[DRY-RUN] Procesaría año {year} ({total_works} artículos con referencias).")
            continue
            
        logger.info(f"🚀 Procesando año {year} ({total_works} artículos fuente)...")
        start_time = time.time()
        
        # Eliminar partición del año para garantizar idempotencia
        try:
            client.command(f"ALTER TABLE rag.work_citations DROP PARTITION {year}")
            logger.info(f"   - Partición {year} eliminada (idempotencia)")
        except Exception as e:
            # Es normal si la partición no existe
            pass
            
        # Ejecutar el unnesting masivo (arrayJoin)
        insert_sql = f"""
            INSERT INTO rag.work_citations
            SELECT 
                arrayJoin(referenced_works) AS cited_work_id,
                id AS citing_work_id,
                publication_year AS citing_publication_year
            FROM rag.works_flat
            WHERE publication_year = {year}
        """
        
        try:
            client.command(insert_sql)
            elapsed = time.time() - start_time
            
            # Verificamos cuantas filas de citas (relaciones) se generaron realmente
            inserted_count = client.query(f"SELECT count() FROM rag.work_citations WHERE citing_publication_year = {year}").first_row[0]
            logger.info(f"   ✅ Año {year} completado: {inserted_count:,} conexiones creadas en {elapsed:.1f}s")
            
        except Exception as e:
            logger.error(f"❌ Error migrando año {year}: {e}")
            break
            
        logger.info(f"   ⏳ Pausa de {PAUSE_BETWEEN_YEARS}s antes del siguiente año...")
        time.sleep(PAUSE_BETWEEN_YEARS)

    logger.info("✨ Población de work_citations finalizada.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Imprime los años que se procesarían sin ejecutar INSERTS.")
    parser.add_argument("--limit", type=int, help="Limita el número de años a procesar (útil para pruebas).")
    args = parser.parse_args()
    
    cli = get_client()
    init_schema(cli)
    run_migration(cli, dry_run=args.dry_run, limit_years=args.limit)
