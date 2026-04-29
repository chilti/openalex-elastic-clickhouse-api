
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
    # Count MX
    res_mx = client.query("SELECT count() FROM rag.institutions_seed_mexico WHERE country_code = 'MX'")
    count_mx = res_mx.result_rows[0][0]
    
    # Count Total
    res_total = client.query("SELECT count() FROM rag.institutions_seed_mexico")
    count_total = res_total.result_rows[0][0]
    
    print(f"Total institutions in table: {count_total:,}")
    print(f"Institutions with country_code='MX': {count_mx:,}")
    print(f"Percentage Mexico: {(count_mx/count_total*100) if count_total > 0 else 0:.2f}%")
    
    # Top 5 non-MX institutions (to see what else is there)
    res_other = client.query("SELECT display_name, country_code FROM rag.institutions_seed_mexico WHERE country_code != 'MX' LIMIT 5")
    print("\nSample of non-Mexico institutions (included because of ROR ID):")
    for row in res_other.result_rows:
        print(f"- {row[0]} ({row[1]})")
        
except Exception as e:
    print(f"Error: {e}")
