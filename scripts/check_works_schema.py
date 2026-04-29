import clickhouse_connect

CH_HOST = "10.90.0.87"
CH_PORT = 8124
CH_USER = "rag_user"
CH_PASSWORD = "$B3tt3r-R4g-3veR-d0N3++"
CH_DATABASE = "rag"

try:
    client = clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASSWORD, 
        database=CH_DATABASE, secure=False
    )
    res = client.query("SHOW CREATE TABLE works")
    print(res.first_row[0])
except Exception as e:
    print(f"Error: {e}")
