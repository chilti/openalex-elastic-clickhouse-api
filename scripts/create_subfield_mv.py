import clickhouse_connect
import os
import logging
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv(r'C:\Users\jlja\Documents\Proyectos\revistaslatam\.env')

client = clickhouse_connect.get_client(
    host=os.environ.get('CH_HOST', '10.90.0.87'),
    port=int(os.environ.get('CH_PORT', 8124)),
    username=os.environ.get('CH_USER', 'rag_user'),
    password=os.environ.get('CH_PASSWORD', ''),
    database=os.environ.get('CH_DATABASE', 'rag'),
    secure=False,
    verify=False
)

def create_mv():
    # 1. Create Target Table
    logger.info("Creating summing_subfield_metrics table...")
    client.command("""
    CREATE TABLE IF NOT EXISTS summing_subfield_metrics (
        subfield String,
        year UInt16,
        country_code String,
        source_id String,
        topic String,
        doc_count UInt64,
        fwci_sum Float64,
        percentile_sum Float64,
        top_10_sum UInt64,
        top_1_sum UInt64,
        gold_count UInt64,
        diamond_count UInt64,
        green_count UInt64,
        hybrid_count UInt64,
        bronze_count UInt64,
        closed_count UInt64,
        lang_en UInt64,
        lang_es UInt64,
        lang_pt UInt64
    ) ENGINE = SummingMergeTree()
    ORDER BY (subfield, year, country_code, source_id, topic)
    """)

    # 2. Populate from existing data (Deduplicated)
    logger.info("Populating summing_subfield_metrics with initial data (this might take a few minutes)...")
    client.command("""
    INSERT INTO summing_subfield_metrics
    SELECT
        subfield,
        toUInt16(publication_year) as year,
        country_code,
        source_id,
        topic,
        count() as doc_count,
        sum(fwci) as fwci_sum,
        sum(percentile) as percentile_sum,
        sum(toUInt64(is_top_10)) as top_10_sum,
        sum(toUInt64(is_top_1)) as top_1_sum,
        sum(toUInt64(oa_status='gold')) as gold_count,
        sum(toUInt64(oa_status='diamond')) as diamond_count,
        sum(toUInt64(oa_status='green')) as green_count,
        sum(toUInt64(oa_status='hybrid')) as hybrid_count,
        sum(toUInt64(oa_status='bronze')) as bronze_count,
        sum(toUInt64(oa_status='closed')) as closed_count,
        sum(toUInt64(language='en')) as lang_en,
        sum(toUInt64(language='es')) as lang_es,
        sum(toUInt64(language='pt')) as lang_pt
    FROM (
        SELECT 
            id,
            argMax(publication_year, updated_date) as publication_year,
            argMax(subfield, updated_date) as subfield,
            argMax(topic, updated_date) as topic,
            argMax(source_id, updated_date) as source_id,
            argMax(country_code, updated_date) as country_code,
            argMax(fwci, updated_date) as fwci,
            argMax(percentile, updated_date) as percentile,
            argMax(is_top_10, updated_date) as is_top_10,
            argMax(is_top_1, updated_date) as is_top_1,
            argMax(oa_status, updated_date) as oa_status,
            argMax(language, updated_date) as language
        FROM works
        GROUP BY id
    )
    WHERE subfield != ''
    GROUP BY subfield, year, country_code, source_id, topic
    """)

    logger.info("MV base table created and populated!")

if __name__ == "__main__":
    create_mv()
