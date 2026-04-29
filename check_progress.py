
import clickhouse_connect
import os
from dotenv import load_dotenv

load_dotenv(r'c:\Users\jlja\Documents\Proyectos\RAGs\.env')

CH_HOST = os.getenv("CH_HOST")
CH_PORT = int(os.getenv("CH_PORT", 8124))
CH_USER = os.getenv("CH_USER")
CH_PASSWORD = os.getenv("CH_PASSWORD")
CH_DB = os.getenv("CH_DATABASE")

try:
    client = clickhouse_connect.get_client(host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASSWORD, database=CH_DB)
    # Get columns of system.processes to be sure
    res = client.query("SELECT * FROM system.processes LIMIT 1")
    cols = res.column_names
    print(f"Columns in system.processes: {cols}")
    
    query = f"SELECT query_id, read_rows, elapsed, query FROM system.processes WHERE query LIKE '%authors_seed_mexico%'"
    res = client.query(query)
    if res.result_rows:
        for row in res.result_rows:
            print(f"Query ID: {row[0]}")
            print(f"Read Rows: {row[1]:,}")
            print(f"Elapsed: {row[2]}s")
    else:
        print("No active query found.")
        # Check table
        try:
            res = client.query("SELECT count() FROM rag.authors_seed_mexico")
            print(f"Table rag.authors_seed_mexico count: {res.result_rows[0][0]:,}")
        except:
            print("Table doesn't exist yet.")
except Exception as e:
    print(f"Error: {e}")
