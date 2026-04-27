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
    logger.info("Limpiando tablas para aplicar nuevo esquema (Abstract + Partitioning)...")
    client.command("DROP TABLE IF EXISTS works_flat_mv")
    client.command("DROP TABLE IF EXISTS works_flat SYNC SETTINGS max_table_size_to_drop = 0")
    
    logger.info("Creando tabla works_flat con particionamiento...")
    create_query = """
    CREATE TABLE IF NOT EXISTS works_flat (
        id String,
        doi String,
        title String,
        abstract String,
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
    PARTITION BY publication_year
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
        arrayStringConcat(
            arrayMap(
                x -> x.1,
                arraySort(
                    x -> x.2,
                    arrayFlatten(
                        arrayMap(
                            (k, v) -> arrayMap(p -> (k, p), v),
                            mapKeys(JSONExtract(raw_data, 'abstract_inverted_index', 'Map(String, Array(Int32))')),
                            mapValues(JSONExtract(raw_data, 'abstract_inverted_index', 'Map(String, Array(Int32))'))
                        )
                    )
                )
            ),
            ' '
        ) as abstract,
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
    
    # Limpieza instantánea gracias al particionamiento
    logger.info(f"Limpiando partición del año {year} (Instantáneo)...")
    try:
        client.command(f"ALTER TABLE works_flat DROP PARTITION '{year}'")
    except Exception as e:
        logger.debug(f"No se pudo borrar partición {year} (posiblemente no existe): {e}")
    
    insert_query = f"""
    INSERT INTO works_flat
    SELECT
        id,
        JSONExtractString(raw_data, 'doi') as doi,
        JSONExtractString(raw_data, 'title') as title,
        arrayStringConcat(
            arrayMap(
                x -> x.1,
                arraySort(
                    x -> x.2,
                    arrayFlatten(
                        arrayMap(
                            (k, v) -> arrayMap(p -> (k, p), v),
                            mapKeys(JSONExtract(raw_data, 'abstract_inverted_index', 'Map(String, Array(Int32))')),
                            mapValues(JSONExtract(raw_data, 'abstract_inverted_index', 'Map(String, Array(Int32))'))
                        )
                    )
                )
            ),
            ' '
        ) as abstract,
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
        
        # 1. Obtener conteos de IDs únicos por año en la tabla plana
        logger.info("Verificando progreso en works_flat (IDs únicos)...")
        res_processed = client.query("SELECT publication_year, count(DISTINCT id) FROM works_flat GROUP BY publication_year").result_rows
        processed_stats = {int(row[0]): int(row[1]) for row in res_processed}
        
        # 2. Obtener conteos de IDs únicos por año en la tabla original (objetivo)
        logger.info("Escaneando trabajos únicos en works (raw) - Esto puede tardar un poco...")
        try:
            raw_stats_res = client.query("SELECT publication_year, count(DISTINCT id) FROM works WHERE publication_year > 0 GROUP BY publication_year").result_rows
        except Exception:
            logger.warning("No se pudo usar la columna 'publication_year' nativa, recurriendo a JSONExtract...")
            raw_stats_res = client.query("SELECT toUInt16(JSONExtractInt(raw_data, 'publication_year')) as yr, count(DISTINCT id) FROM works WHERE yr > 0 GROUP BY yr").result_rows
            
        raw_stats = {int(row[0]): int(row[1]) for row in raw_stats_res}
        
        all_years = sorted(raw_stats.keys(), reverse=True)
        
        # Filtramos: se procesa si el año no está o si el conteo de únicos es menor al original
        years_to_process = []
        for y in all_years:
            target_count = raw_stats[y]
            current_count = processed_stats.get(y, 0)
            
            # Si faltan más de 10 registros, re-procesamos (tolerancia para desajustes mínimos)
            if current_count < (target_count - 10):
                if current_count > 0:
                    logger.info(f"▶️ Año {y} incompleto: {current_count} únicos en flat vs {target_count} únicos en raw. Se re-procesará.")
                else:
                    logger.info(f"🆕 Año {y}: pendiente de migrar ({target_count} únicos).")
                years_to_process.append(y)
            else:
                if y >= 2000:
                    logger.info(f"✅ Año {y} ya está completo ({current_count} únicos).")
        
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
