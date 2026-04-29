
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
    res = client.query("SHOW DATABASES")
    print("Available databases:")
    for row in res.result_rows:
        print(f"- {row[0]}")
except Exception as e:
    print(f"Error: {e}")
