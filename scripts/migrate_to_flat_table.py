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

# ── Pausa entre años (segundos) para no saturar ClickHouse ─────────────────────
# Ajustar según la capacidad del servidor. 30s es conservador.
PAUSE_BETWEEN_YEARS = 30

def get_client():
    return clickhouse_connect.get_client(
        host=CH_HOST,
        port=CH_PORT,
        username=CH_USER,
        password=CH_PASSWORD,
        database=CH_DATABASE,
        send_receive_timeout=900  # 15 minutos por operación
    )


# ── SQL compartido para extraer todos los campos ───────────────────────────────
# Este bloque SELECT se reutiliza en el INSERT y en la Materialized View.
# Debe producir exactamente 70 columnas en el mismo orden que la tabla works_flat.
SELECT_FIELDS = """
        id,
        JSONExtractString(raw_data, 'doi')                                                     AS doi,
        JSONExtractString(raw_data, 'title')                                                   AS title,
        arrayStringConcat(arrayMap(x -> x.1, arraySort(x -> x.2, arrayFlatten(arrayMap(
            (k, v) -> arrayMap(p -> (k, p), v),
            mapKeys(JSONExtract(raw_data, 'abstract_inverted_index', 'Map(String, Array(Int32))')),
            mapValues(JSONExtract(raw_data, 'abstract_inverted_index', 'Map(String, Array(Int32))'))
        )))), ' ')                                                                               AS abstract,
        toUInt16(JSONExtractInt(raw_data, 'publication_year'))                                 AS publication_year,
        parseDateTimeBestEffortOrZero(JSONExtractString(raw_data, 'publication_date'))         AS publication_date,
        JSONExtractString(raw_data, 'type')                                                    AS type,
        JSONExtractString(raw_data, 'language')                                                AS language,
        toUInt32(JSONExtractInt(raw_data, 'cited_by_count'))                                   AS cited_by_count,
        toFloat32(JSONExtractFloat(raw_data, 'fwci'))                                          AS fwci,
        toFloat32(JSONExtractFloat(raw_data, 'citation_normalized_percentile', 'value')) * 100 AS percentile,
        toUInt8(JSONExtractBool(raw_data, 'citation_normalized_percentile', 'is_in_top_10_percent')) AS is_top_10,
        toUInt8(JSONExtractBool(raw_data, 'citation_normalized_percentile', 'is_in_top_1_percent'))  AS is_top_1,
        toUInt32(JSONExtractInt(raw_data, 'referenced_works_count'))                           AS referenced_works_count,
        JSONExtractString(raw_data, 'primary_location', 'source', 'id')                       AS source_id,
        JSONExtractString(raw_data, 'primary_location', 'source', 'type')                     AS source_type,
        toUInt8(JSONExtractBool(raw_data, 'open_access', 'is_oa'))                            AS is_oa,
        JSONExtractString(raw_data, 'open_access', 'oa_status')                               AS oa_status,
        JSONExtractString(raw_data, 'primary_topic', 'id')                                    AS topic_id,
        JSONExtractString(raw_data, 'primary_topic', 'subfield', 'id')                        AS subfield_id,
        JSONExtractString(raw_data, 'primary_topic', 'subfield', 'display_name')              AS subfield_name,
        JSONExtractString(raw_data, 'primary_topic', 'field', 'display_name')                 AS field_name,
        JSONExtractString(raw_data, 'primary_topic', 'domain', 'display_name')                AS domain_name,
        JSONExtract(raw_data, 'authorships', 'Array(Tuple(author Tuple(id String)))').author.id AS author_ids,
        arrayFlatten(JSONExtract(raw_data, 'authorships', 'Array(Tuple(institutions Array(Tuple(id String))))').institutions.id) AS institution_ids,
        arrayFlatten(JSONExtract(raw_data, 'authorships', 'Array(Tuple(institutions Array(Tuple(type String))))').institutions.type) AS institution_types,
        arrayDistinct(arrayFlatten(JSONExtract(raw_data, 'authorships', 'Array(Tuple(countries Array(String)))').countries)) AS country_codes,
        JSONExtract(raw_data, 'referenced_works', 'Array(String)')                            AS referenced_works,
        JSONExtract(raw_data, 'concepts', 'Array(Tuple(display_name String))').display_name   AS concepts,
        JSONExtractString(raw_data, 'ids', 'pmid')                                             AS pmid,
        JSONExtractString(raw_data, 'ids', 'mag')                                              AS mag_id,
        toUInt8(JSONExtractBool(raw_data, 'is_retracted'))                                    AS is_retracted,
        toUInt8(JSONExtractBool(raw_data, 'is_paratext'))                                     AS is_paratext,
        JSONExtractString(raw_data, 'biblio', 'volume')                                        AS volume,
        JSONExtractString(raw_data, 'biblio', 'issue')                                         AS issue,
        JSONExtractString(raw_data, 'biblio', 'first_page')                                    AS first_page,
        JSONExtractString(raw_data, 'biblio', 'last_page')                                     AS last_page,
        JSONExtract(raw_data, 'topics', 'Array(Tuple(id String))').id                         AS all_topics,
        JSONExtract(raw_data, 'keywords', 'Array(Tuple(display_name String))').display_name   AS keywords,
        JSONExtract(raw_data, 'mesh', 'Array(Tuple(descriptor_name String))').descriptor_name AS mesh,
        JSONExtract(raw_data, 'funders', 'Array(Tuple(id String))').id                        AS funder_ids,
        JSONExtract(raw_data, 'funders', 'Array(Tuple(display_name String))').display_name    AS funder_names,
        JSONExtract(raw_data, 'sustainable_development_goals', 'Array(Tuple(id String))').id  AS sdgs,
        -- ── Campos desnormalizados (compatibilidad con seed/API) ─────────────
        raw_data                                                                               AS raw_data,
        JSONExtractString(raw_data, 'updated_date')                                           AS updated_date,
        JSONExtractString(raw_data, 'is_xpac')                                                AS is_xpac,
        arrayDistinct(arrayFlatten(arrayMap(
            x -> [(x.1).1, x.2],
            JSONExtract(raw_data, 'authorships', 'Array(Tuple(author Tuple(display_name String), raw_author_name String))')
        )))                                                                                    AS author_names,
        arrayDistinct(arrayFlatten(arrayMap(
            x -> arrayMap(i -> JSONExtractString(i, 'ror'), JSONExtractArrayRaw(x, 'institutions')),
            JSONExtractArrayRaw(raw_data, 'authorships')
        )))                                                                                    AS institution_rors,
        arrayDistinct(arrayFlatten(arrayMap(
            x -> arrayMap(i -> JSONExtractString(i, 'display_name'), JSONExtractArrayRaw(x, 'institutions')),
            JSONExtractArrayRaw(raw_data, 'authorships')
        )))                                                                                    AS institution_names,
        JSONExtractString(raw_data, 'primary_topic', 'id')                                    AS primary_topic_id,
        JSONExtractString(raw_data, 'primary_topic', 'subfield', 'display_name')              AS subfield,
        JSONExtractString(raw_data, 'primary_topic', 'field', 'display_name')                 AS field,
        JSONExtractString(raw_data, 'primary_topic', 'domain', 'display_name')                AS domain,
        JSONExtractString(raw_data, 'primary_topic', 'display_name')                          AS topic,
        JSONExtractString(raw_data, 'authorships', 1, 'institutions', 1, 'country_code')      AS country_code,
        arrayMap(x -> x.1, arrayFilter(x -> x.3 >= 0.4,
            JSONExtract(raw_data, 'sustainable_development_goals', 'Array(Tuple(String, String, Float32))')
        ))                                                                                     AS sdg_ids,
        arrayMap(x -> x.3, JSONExtract(raw_data, 'grants', 'Array(Tuple(String, String, String))')) AS awards,
        arrayMap(x -> x.1, JSONExtract(raw_data, 'concepts', 'Array(Tuple(String, String, String, Int8, Float32))')) AS concept_ids,
        arrayDistinct(arrayFlatten(arrayMap(
            x -> arrayMap(i -> JSONExtractString(i, 'country_code'), JSONExtractArrayRaw(x, 'institutions')),
            JSONExtractArrayRaw(raw_data, 'authorships')
        )))                                                                                    AS all_country_codes,
        -- ── APC y enriquecimiento externo ────────────────────────────────────
        toFloat64OrZero(JSONExtractString(raw_data, 'apc_paid', 'value_usd'))                 AS apc_paid_usd,
        toFloat64OrZero(JSONExtractString(raw_data, 'apc_list', 'value_usd'))                 AS apc_list_usd,
        ifNull(JSONExtractRaw(raw_data, 'counts_by_year'), '[]')                              AS counts_by_year,
        toUInt8(0)                                                                             AS is_doaj_indexed,
        toUInt8(0)                                                                             AS is_doaj_journal,
        toUInt8(0)                                                                             AS is_core_journal,
        toUInt8(JSONExtractBool(raw_data, 'has_fulltext'))                                    AS has_repository_fulltext,
        JSONExtractString(raw_data, 'primary_location', 'license')                            AS license,
        toUInt8(0)                                                                             AS journal_is_in_doaj,
        toUInt8(0)                                                                             AS journal_is_core,
        toUInt8(JSONExtractBool(raw_data, 'open_access', 'any_repository_has_fulltext'))      AS any_repository_has_fulltext
"""


def ensure_flat_table(client):
    """
    Crea works_flat y works_flat_mv si no existen.
    Si la MV ya está (incluso en estado detached), simplemente la omite —
    la migración manual no la necesita.
    """
    logger.info("Creando works_flat con esquema completo (70 columnas) si no existe...")
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS rag.works_flat (
        id                       String,
        doi                      String,
        title                    String,
        abstract                 String,
        publication_year         UInt16,
        publication_date         Date,
        type                     LowCardinality(String),
        language                 LowCardinality(String),
        cited_by_count           UInt32,
        fwci                     Float32,
        percentile               Float32,
        is_top_10                UInt8,
        is_top_1                 UInt8,
        referenced_works_count   UInt32,
        source_id                LowCardinality(String),
        source_type              LowCardinality(String),
        is_oa                    UInt8,
        oa_status                LowCardinality(String),
        topic_id                 LowCardinality(String),
        subfield_id              LowCardinality(String),
        subfield_name            LowCardinality(String),
        field_name               LowCardinality(String),
        domain_name              LowCardinality(String),
        author_ids               Array(String),
        institution_ids          Array(String),
        institution_types        Array(LowCardinality(String)),
        country_codes            Array(LowCardinality(String)),
        referenced_works         Array(String),
        concepts                 Array(LowCardinality(String)),
        pmid                     String,
        mag_id                   String,
        is_retracted             UInt8,
        is_paratext              UInt8,
        volume                   String,
        issue                    String,
        first_page               String,
        last_page                String,
        all_topics               Array(LowCardinality(String)),
        keywords                 Array(String),
        mesh                     Array(String),
        funder_ids               Array(String),
        funder_names             Array(String),
        sdgs                     Array(LowCardinality(String)),
        raw_data                 String,
        updated_date             String,
        is_xpac                  String,
        author_names             Array(String),
        institution_rors         Array(String),
        institution_names        Array(String),
        primary_topic_id         String,
        subfield                 String,
        field                    String,
        domain                   String,
        topic                    String,
        country_code             String,
        sdg_ids                  Array(String),
        awards                   Array(String),
        concept_ids              Array(String),
        all_country_codes        Array(String),
        apc_paid_usd             Float64,
        apc_list_usd             Float64,
        counts_by_year           String,
        is_doaj_indexed          UInt8,
        is_doaj_journal          UInt8,
        is_core_journal          UInt8,
        has_repository_fulltext  UInt8,
        license                  String,
        journal_is_in_doaj       UInt8,
        journal_is_core          UInt8,
        any_repository_has_fulltext UInt8
    ) ENGINE = ReplacingMergeTree()
    PARTITION BY publication_year
    ORDER BY id
    SETTINGS index_granularity = 8192
    """
    client.command(create_sql)
    logger.info("✅ works_flat lista.")

    logger.info("Intentando crear Materialized View works_flat_mv...")
    mv_sql = f"""
    CREATE MATERIALIZED VIEW IF NOT EXISTS rag.works_flat_mv TO rag.works_flat AS
    SELECT
    {SELECT_FIELDS}
    FROM rag.works
    """
    try:
        client.command(mv_sql)
        logger.info("✅ works_flat_mv creada (los nuevos inserts en works se reflejarán automáticamente).")
    except Exception as e:
        logger.warning(f"⚠️  No se pudo crear works_flat_mv (puede existir en estado detached): {e}")
        logger.warning("   La migración manual continuará sin problema — la MV no es necesaria para el INSERT.")


def migrate_batch(client, year):
    """Migra un año completo de works -> works_flat. Incluye DROP PARTITION para reanudación segura."""
    logger.info(f"🚀 Procesando año {year}...")

    # Borrar partición si ya existe (para poder re-ejecutar de forma idempotente)
    try:
        client.command(f"ALTER TABLE rag.works_flat DROP PARTITION '{year}'")
        logger.debug(f"   Partición {year} limpiada.")
    except Exception as e:
        logger.debug(f"   No se pudo limpiar partición {year} (posiblemente no existe): {e}")

    insert_sql = f"""
    INSERT INTO rag.works_flat
    SELECT
    {SELECT_FIELDS}
    FROM rag.works
    WHERE publication_year = {year}
    """

    t0 = time.time()
    client.command(insert_sql)
    elapsed = time.time() - t0

    count = client.query(f"SELECT count() FROM rag.works_flat WHERE publication_year = {year}").first_row[0]
    logger.info(f"   ✅ Año {year}: {count:,} filas migradas en {elapsed:.1f}s")


def main():
    client = get_client()

    # ── Verificar si works_flat está vacía para decidir si recrear ────────────
    try:
        flat_count = client.query("SELECT count() FROM rag.works_flat").first_row[0]
    except Exception:
        flat_count = 0

    ensure_flat_table(client)
    if flat_count == 0:
        logger.info("works_flat está vacía — se llenará con la migración.")
    else:
        logger.info(f"works_flat ya tiene {flat_count:,} filas. Iniciando en modo reanudación.")

    # ── Obtener años disponibles en works (raw) ───────────────────────────────
    logger.info("Escaneando años disponibles en works (raw)...")
    raw_stats_res = client.query(
        "SELECT publication_year, count(DISTINCT id) AS n "
        "FROM rag.works "
        "WHERE publication_year > 0 "
        "GROUP BY publication_year "
        "ORDER BY publication_year DESC"
    ).result_rows
    raw_stats = {int(row[0]): int(row[1]) for row in raw_stats_res}

    # ── Obtener progreso actual en works_flat ─────────────────────────────────
    logger.info("Verificando progreso en works_flat...")
    flat_stats_res = client.query(
        "SELECT publication_year, count(DISTINCT id) AS n "
        "FROM rag.works_flat "
        "GROUP BY publication_year"
    ).result_rows
    flat_stats = {int(row[0]): int(row[1]) for row in flat_stats_res}

    # ── Decidir qué años procesar ─────────────────────────────────────────────
    years_to_process = []
    for year, target in raw_stats.items():
        done = flat_stats.get(year, 0)
        if done < (target - 10):  # tolerancia de 10 registros
            years_to_process.append((year, target, done))

    if not years_to_process:
        logger.info("🎉 ¡Todos los años ya están migrados!")
        return

    # Ordenar: primero los años con más volumen (recientes) y los históricos al final
    years_to_process.sort(key=lambda x: x[0], reverse=True)

    logger.info(f"Pendientes: {len(years_to_process)} años. Pausa entre años: {PAUSE_BETWEEN_YEARS}s")
    logger.info("Años a procesar (año, objetivo, ya migrado):")
    for y, t, d in years_to_process:
        logger.info(f"   {y}: {d:,}/{t:,}")

    # ── Migración año por año ─────────────────────────────────────────────────
    for i, (year, target, done) in enumerate(years_to_process):
        try:
            migrate_batch(client, year)
        except Exception as e:
            logger.error(f"❌ Error en año {year}: {e}. Continuando con el siguiente...")

        # Pausa controlada entre años para no saturar ClickHouse
        if i < len(years_to_process) - 1:
            logger.info(f"   ⏳ Pausa de {PAUSE_BETWEEN_YEARS}s antes del siguiente año...")
            time.sleep(PAUSE_BETWEEN_YEARS)

    logger.info("✨ Migración finalizada. Verificando totales...")
    total_flat = client.query("SELECT count() FROM rag.works_flat").first_row[0]
    total_works = client.query("SELECT count() FROM rag.works").first_row[0]
    logger.info(f"   works (raw): {total_works:,}")
    logger.info(f"   works_flat:  {total_flat:,}")


if __name__ == "__main__":
    main()
