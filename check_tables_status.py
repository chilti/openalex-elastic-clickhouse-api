
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
    res = client.query("SELECT name FROM system.tables WHERE database = 'rag' AND name LIKE '%seed_mexico%'")
    tables = [row[0] for row in res.result_rows]
    print(f"Tables found in 'rag' database: {tables}")
    
    for table in tables:
        count_res = client.query(f"SELECT count() FROM rag.{table}")
        print(f"Table {table} count: {count_res.result_rows[0][0]:,}")
except Exception as e:
    print(f"Error: {e}")
