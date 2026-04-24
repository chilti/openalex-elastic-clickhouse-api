import clickhouse_connect
import os
import time
from dotenv import load_dotenv

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

subfield = "Pulmonary and Respiratory Medicine"

query_old = f"""
SELECT 
    year, 
    country_code, 
    journal_id, 
    topic,
    count() as doc_count
FROM (
    SELECT 
        id,
        argMax(toUInt16(JSONExtractInt(raw_data, 'publication_year')), updated_date) as year,
        argMax(JSONExtractString(raw_data, 'primary_topic', 'subfield', 'display_name'), updated_date) as subfield,
        argMax(JSONExtractString(raw_data, 'primary_topic', 'display_name'), updated_date) as topic,
        argMax(JSONExtractString(raw_data, 'primary_location', 'source', 'id'), updated_date) as journal_id,
        argMax(JSONExtractString(raw_data, 'authorships', 1, 'institutions', 1, 'country_code'), updated_date) as country_code
    FROM works
    GROUP BY id
)
WHERE subfield = '{subfield}' AND year >= 2000
GROUP BY year, country_code, journal_id, topic
LIMIT 10
"""

query_new = f"""
SELECT 
    publication_year as year, 
    country_code, 
    source_id as journal_id, 
    topic,
    count() as doc_count
FROM (
    SELECT 
        id,
        argMax(publication_year, updated_date) as publication_year,
        argMax(subfield, updated_date) as subfield,
        argMax(topic, updated_date) as topic,
        argMax(source_id, updated_date) as source_id,
        argMax(country_code, updated_date) as country_code
    FROM works
    GROUP BY id
)
WHERE subfield = '{subfield}' AND year >= 2000
GROUP BY year, country_code, journal_id, topic
LIMIT 10
"""

def test_query(name, query):
    print(f"Running {name}...")
    start = time.time()
    try:
        res = client.query(query)
        end = time.time()
        print(f"{name} took {end - start:.4f} seconds. Rows: {len(res.result_rows)}")
    except Exception as e:
        print(f"Error in {name}: {e}")

if __name__ == "__main__":
    test_query("OLD (JSON)", query_old)
    test_query("NEW (Materialized)", query_new)
