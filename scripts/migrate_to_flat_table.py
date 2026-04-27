import clickhouse_connect
import os
import logging
import time
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

def get_client():
    return clickhouse_connect.get_client(
        host=CH_HOST,
        port=CH_PORT,
        username=CH_USER,
        password=CH_PASSWORD,
        database=CH_DATABASE,
        send_receive_timeout=900 # Aumentado a 15 minutos para evitar timeouts en años grandes
    )

def create_flat_table(client):
    # Ya no borramos la tabla automáticamente para permitir reanudación
    # client.command("DROP TABLE IF EXISTS works_flat_mv")
    # client.command("DROP TABLE IF EXISTS works_flat")
    
    logger.info("Verificando/Creando tabla works_flat...")
    create_query = """
    CREATE TABLE IF NOT EXISTS works_flat (
        id String,
        doi String,
        title String,
        publication_year UInt16,
        publication_date Date,
        type LowCardinality(String),
        language LowCardinality(String),

        cited_by_count UInt32,
        fwci Float32,
        percentile Float32,
        is_top_10 UInt8,
        is_top_1 UInt8,
        referenced_works_count UInt32,

        source_id LowCardinality(String),
        source_type LowCardinality(String),
        is_oa UInt8,
        oa_status LowCardinality(String),

        topic_id LowCardinality(String),
        subfield_id LowCardinality(String),
        subfield_name LowCardinality(String),
        field_name LowCardinality(String),
        domain_name LowCardinality(String),

        author_ids Array(String),
        institution_ids Array(String),
        institution_types Array(LowCardinality(String)),
        country_codes Array(LowCardinality(String)),
        referenced_works Array(String),
        concepts Array(LowCardinality(String)),
        
        -- Nuevos campos
        pmid String,
        mag_id String,
        is_retracted UInt8,
        is_paratext UInt8,
        volume String,
        issue String,
        first_page String,
        last_page String,
        all_topics Array(LowCardinality(String)),
        keywords Array(String),
        mesh Array(String),
        funder_ids Array(String),
        funder_names Array(String),
        sdgs Array(LowCardinality(String))

    ) ENGINE = ReplacingMergeTree()
    ORDER BY (id)
    """
    client.command(create_query)
    
    # Creamos la Materialized View para sincronización automática
    logger.info("Creando Materialized View works_flat_mv...")
    mv_query = """
    CREATE MATERIALIZED VIEW IF NOT EXISTS works_flat_mv TO works_flat AS
    SELECT
        id,
        JSONExtractString(raw_data, 'doi') as doi,
        JSONExtractString(raw_data, 'title') as title,
        toUInt16(JSONExtractInt(raw_data, 'publication_year')) as publication_year,
        parseDateTimeBestEffortOrZero(JSONExtractString(raw_data, 'publication_date')) as publication_date,
        JSONExtractString(raw_data, 'type') as type,
        JSONExtractString(raw_data, 'language') as language,
        
        toUInt32(JSONExtractInt(raw_data, 'cited_by_count')) as cited_by_count,
        toFloat32(JSONExtractFloat(raw_data, 'fwci')) as fwci,
        toFloat32(JSONExtractFloat(raw_data, 'citation_normalized_percentile', 'value')) * 100 as percentile,
        toUInt8(JSONExtractBool(raw_data, 'citation_normalized_percentile', 'is_in_top_10_percent')) as is_top_10,
        toUInt8(JSONExtractBool(raw_data, 'citation_normalized_percentile', 'is_in_top_1_percent')) as is_top_1,
        toUInt32(JSONExtractInt(raw_data, 'referenced_works_count')) as referenced_works_count,
        
        JSONExtractString(raw_data, 'primary_location', 'source', 'id') as source_id,
        JSONExtractString(raw_data, 'primary_location', 'source', 'type') as source_type,
        toUInt8(JSONExtractBool(raw_data, 'open_access', 'is_oa')) as is_oa,
        JSONExtractString(raw_data, 'open_access', 'oa_status') as oa_status,
        
        JSONExtractString(raw_data, 'primary_topic', 'id') as topic_id,
        JSONExtractString(raw_data, 'primary_topic', 'subfield', 'id') as subfield_id,
        JSONExtractString(raw_data, 'primary_topic', 'subfield', 'display_name') as subfield_name,
        JSONExtractString(raw_data, 'primary_topic', 'field', 'display_name') as field_name,
        JSONExtractString(raw_data, 'primary_topic', 'domain', 'display_name') as domain_name,
        
        JSONExtract(raw_data, 'authorships', 'Array(Tuple(author Tuple(id String)))').author.id as author_ids,
        arrayFlatten(JSONExtract(raw_data, 'authorships', 'Array(Tuple(institutions Array(Tuple(id String))))').institutions.id) as institution_ids,
        arrayFlatten(JSONExtract(raw_data, 'authorships', 'Array(Tuple(institutions Array(Tuple(type String))))').institutions.type) as institution_types,
        arrayDistinct(arrayFlatten(JSONExtract(raw_data, 'authorships', 'Array(Tuple(countries Array(String)))').countries)) as country_codes,
        JSONExtract(raw_data, 'referenced_works', 'Array(String)') as referenced_works,
        JSONExtract(raw_data, 'concepts', 'Array(Tuple(display_name String))').display_name as concepts,
        
        -- Nuevos campos
        JSONExtractString(raw_data, 'ids', 'pmid') as pmid,
        JSONExtractString(raw_data, 'ids', 'mag') as mag_id,
        toUInt8(JSONExtractBool(raw_data, 'is_retracted')) as is_retracted,
        toUInt8(JSONExtractBool(raw_data, 'is_paratext')) as is_paratext,
        JSONExtractString(raw_data, 'biblio', 'volume') as volume,
        JSONExtractString(raw_data, 'biblio', 'issue') as issue,
        JSONExtractString(raw_data, 'biblio', 'first_page') as first_page,
        JSONExtractString(raw_data, 'biblio', 'last_page') as last_page,
        JSONExtract(raw_data, 'topics', 'Array(Tuple(id String))').id as all_topics,
        JSONExtract(raw_data, 'keywords', 'Array(Tuple(display_name String))').display_name as keywords,
        JSONExtract(raw_data, 'mesh', 'Array(Tuple(descriptor_name String))').descriptor_name as mesh,
        JSONExtract(raw_data, 'funders', 'Array(Tuple(id String))').id as funder_ids,
        JSONExtract(raw_data, 'funders', 'Array(Tuple(display_name String))').display_name as funder_names,
        JSONExtract(raw_data, 'sustainable_development_goals', 'Array(Tuple(id String))').id as sdgs
    FROM works
    """
    client.command(mv_query)

def migrate_batch(client, year):
    logger.info(f"🚀 Procesando año {year}...")
    
    # Limpiamos datos parciales del año antes de reintentar (por si falló a mitad)
    logger.info(f"Limpiando posibles datos previos del año {year}...")
    client.command(f"ALTER TABLE works_flat DELETE WHERE publication_year = {year}")
    
    # Esperar un momento a que la mutación se procese (opcional pero recomendado en CH)
    time.sleep(2)
    
    insert_query = f"""
    INSERT INTO works_flat
    SELECT
        id,
        JSONExtractString(raw_data, 'doi') as doi,
        JSONExtractString(raw_data, 'title') as title,
        toUInt16(JSONExtractInt(raw_data, 'publication_year')) as publication_year,
        parseDateTimeBestEffortOrZero(JSONExtractString(raw_data, 'publication_date')) as publication_date,
        JSONExtractString(raw_data, 'type') as type,
        JSONExtractString(raw_data, 'language') as language,
        
        toUInt32(JSONExtractInt(raw_data, 'cited_by_count')) as cited_by_count,
        toFloat32(JSONExtractFloat(raw_data, 'fwci')) as fwci,
        toFloat32(JSONExtractFloat(raw_data, 'citation_normalized_percentile', 'value')) * 100 as percentile,
        toUInt8(JSONExtractBool(raw_data, 'citation_normalized_percentile', 'is_in_top_10_percent')) as is_top_10,
        toUInt8(JSONExtractBool(raw_data, 'citation_normalized_percentile', 'is_in_top_1_percent')) as is_top_1,
        toUInt32(JSONExtractInt(raw_data, 'referenced_works_count')) as referenced_works_count,
        
        JSONExtractString(raw_data, 'primary_location', 'source', 'id') as source_id,
        JSONExtractString(raw_data, 'primary_location', 'source', 'type') as source_type,
        toUInt8(JSONExtractBool(raw_data, 'open_access', 'is_oa')) as is_oa,
        JSONExtractString(raw_data, 'open_access', 'oa_status') as oa_status,
        
        JSONExtractString(raw_data, 'primary_topic', 'id') as topic_id,
        JSONExtractString(raw_data, 'primary_topic', 'subfield', 'id') as subfield_id,
        JSONExtractString(raw_data, 'primary_topic', 'subfield', 'display_name') as subfield_name,
        JSONExtractString(raw_data, 'primary_topic', 'field', 'display_name') as field_name,
        JSONExtractString(raw_data, 'primary_topic', 'domain', 'display_name') as domain_name,
        
        JSONExtract(raw_data, 'authorships', 'Array(Tuple(author Tuple(id String)))').author.id as author_ids,
        arrayFlatten(JSONExtract(raw_data, 'authorships', 'Array(Tuple(institutions Array(Tuple(id String))))').institutions.id) as institution_ids,
        arrayFlatten(JSONExtract(raw_data, 'authorships', 'Array(Tuple(institutions Array(Tuple(type String))))').institutions.type) as institution_types,
        arrayDistinct(arrayFlatten(JSONExtract(raw_data, 'authorships', 'Array(Tuple(countries Array(String)))').countries)) as country_codes,
        JSONExtract(raw_data, 'referenced_works', 'Array(String)') as referenced_works,
        JSONExtract(raw_data, 'concepts', 'Array(Tuple(display_name String))').display_name as concepts,
        
        -- Nuevos campos
        JSONExtractString(raw_data, 'ids', 'pmid') as pmid,
        JSONExtractString(raw_data, 'ids', 'mag') as mag_id,
        toUInt8(JSONExtractBool(raw_data, 'is_retracted')) as is_retracted,
        toUInt8(JSONExtractBool(raw_data, 'is_paratext')) as is_paratext,
        JSONExtractString(raw_data, 'biblio', 'volume') as volume,
        JSONExtractString(raw_data, 'biblio', 'issue') as issue,
        JSONExtractString(raw_data, 'biblio', 'first_page') as first_page,
        JSONExtractString(raw_data, 'biblio', 'last_page') as last_page,
        JSONExtract(raw_data, 'topics', 'Array(Tuple(id String))').id as all_topics,
        JSONExtract(raw_data, 'keywords', 'Array(Tuple(display_name String))').display_name as keywords,
        JSONExtract(raw_data, 'mesh', 'Array(Tuple(descriptor_name String))').descriptor_name as mesh,
        JSONExtract(raw_data, 'funders', 'Array(Tuple(id String))').id as funder_ids,
        JSONExtract(raw_data, 'funders', 'Array(Tuple(display_name String))').display_name as funder_names,
        JSONExtract(raw_data, 'sustainable_development_goals', 'Array(Tuple(id String))').id as sdgs
    FROM works
    WHERE toUInt16(JSONExtractInt(raw_data, 'publication_year')) = {year}
    """
    
    start_time = time.time()
    client.command(insert_query)
    end_time = time.time()
    
    logger.info(f"✅ Año {year} completado en {end_time - start_time:.2f} segundos.")

def main():
    try:
        client = get_client()
        create_flat_table(client)
        
        # 1. Obtener conteos por año en la tabla plana (migrados)
        logger.info("Verificando progreso en works_flat...")
        res_processed = client.query("SELECT publication_year, count() FROM works_flat GROUP BY publication_year").result_rows
        processed_stats = {row[0]: row[1] for row in res_processed}
        
        # 2. Obtener conteos por año en la tabla original (objetivo)
        # Usamos la columna nativa 'publication_year' que debe estar en 'works' para mayor velocidad
        logger.info("Escaneando años y conteos en works (raw)...")
        try:
            raw_stats_res = client.query("SELECT publication_year, count() FROM works WHERE publication_year > 0 GROUP BY publication_year").result_rows
        except Exception:
            logger.warning("No se pudo usar la columna 'publication_year' nativa, recurriendo a JSONExtract (más lento)...")
            raw_stats_res = client.query("SELECT toUInt16(JSONExtractInt(raw_data, 'publication_year')) as yr, count() FROM works WHERE yr > 0 GROUP BY yr").result_rows
            
        raw_stats = {int(row[0]): int(row[1]) for row in raw_stats_res}
        
        all_years = sorted(raw_stats.keys(), reverse=True)
        
        # Filtramos: se procesa si el año no está o si el conteo es menor al original (migración incompleta)
        years_to_process = []
        for y in all_years:
            target_count = raw_stats[y]
            current_count = int(processed_stats.get(y, 0))
            
            if current_count < target_count:
                if current_count > 0:
                    logger.info(f"▶️ Año {y} incompleto: {current_count} en flat vs {target_count} en raw. Se re-procesará.")
                else:
                    logger.info(f"🆕 Año {y}: pendiente de migrar ({target_count} registros).")
                years_to_process.append(y)
            else:
                # Opcional: log de saltado para dar visibilidad
                if y >= 2000: # Solo loguear años recientes para no saturar
                    logger.info(f"✅ Año {y} ya está completo ({current_count} registros).")
        
        if not years_to_process:
            logger.info("🎉 ¡Todos los años parecen estar migrados!")
            return

        logger.info(f"Pendientes: {len(years_to_process)} años. Iniciando migración masiva...")
        
        for year in years_to_process:
            migrate_batch(client, year)
            time.sleep(1) # Pausa mínima
            
        logger.info("✨ Proceso finalizado correctamente.")
        
    except Exception as e:
        logger.error(f"Error durante la migración: {e}")

if __name__ == "__main__":
    main()
