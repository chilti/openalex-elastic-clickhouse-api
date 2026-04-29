import clickhouse_connect
import time

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
    
    print("Testing native column...")
    t1 = time.time()
    res1 = client.query("SELECT count() FROM works WHERE publication_year = 1873")
    t2 = time.time()
    print(f"Native column returned {res1.first_row[0]} in {t2 - t1:.2f}s")
    
    print("Testing JSON extraction...")
    t3 = time.time()
    res2 = client.query("SELECT count() FROM works WHERE toUInt16(JSONExtractInt(raw_data, 'publication_year')) = 1873")
    t4 = time.time()
    print(f"JSON extraction returned {res2.first_row[0]} in {t4 - t3:.2f}s")
    
except Exception as e:
    print(f"Error: {e}")
