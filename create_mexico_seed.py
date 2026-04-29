
import clickhouse_connect
import os
import time
import threading
from dotenv import load_dotenv

# Load .env
load_dotenv(r'c:\Users\jlja\Documents\Proyectos\RAGs\.env')

CH_HOST = os.getenv("CH_HOST")
CH_PORT = int(os.getenv("CH_PORT", 8124))
CH_USER = os.getenv("CH_USER")
CH_PASSWORD = os.getenv("CH_PASSWORD")
CH_DB = os.getenv("CH_DATABASE")

query_id = f"create_authors_seed_mx_{int(time.time())}"
target_table = "rag.authors_seed_mexico"

create_query = f"""
CREATE TABLE IF NOT EXISTS {target_table}
ENGINE = MergeTree()
ORDER BY (display_name, id)
AS
SELECT 
    id, 
    display_name, 
    orcid, 
    last_known_institution_id,
    ids,
    raw_data
FROM {CH_DB}.authors
WHERE last_known_institution_country_code = 'MX'
   OR lower(raw_data) LIKE '%mexico%' 
   OR lower(raw_data) LIKE '%unam%'
   OR lower(raw_data) LIKE '%ipn%'
"""

def monitor():
    client = clickhouse_connect.get_client(host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASSWORD, database=CH_DB)
    print(f"Monitoring query_id: {query_id}")
    finished = False
    while not finished:
        try:
            res = client.query(f"SELECT read_rows, total_rows_to_read, elapsed FROM system.processes WHERE query_id = '{query_id}'")
            if res.result_rows:
                row = res.result_rows[0]
                read_rows = row[0]
                total = row[1]
                elapsed = row[2]
                percent = (read_rows / total * 100) if total > 0 else 0
                print(f"[{elapsed:.1f}s] Progress: {percent:.2f}% ({read_rows:,}/{total:,} rows)")
            else:
                # Check if it's finished by checking if the table exists and has rows
                res = client.query(f"SELECT count() FROM {target_table}")
                count = res.result_rows[0][0]
                if count > 0:
                    print(f"Query finished! Total rows in {target_table}: {count:,}")
                    finished = True
        except Exception as e:
            # Table might not exist yet or count might fail if it's being created
            pass
        if not finished:
            time.sleep(10)

def run_query():
    client = clickhouse_connect.get_client(host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASSWORD, database=CH_DB)
    print(f"Starting CREATE TABLE {target_table} AS SELECT...")
    try:
        client.command(create_query, query_id=query_id)
    except Exception as e:
        print(f"Query execution stopped or finished: {e}")

monitor_thread = threading.Thread(target=monitor)
monitor_thread.start()

run_query()
monitor_thread.join()
