import clickhouse_connect
import os
from dotenv import load_dotenv

# Load from the project directory
load_dotenv(r'C:\Users\jlja\Documents\Proyectos\revistaslatam\.env')

CH_HOST = os.environ.get('CH_HOST', '10.90.0.87')
CH_PORT = int(os.environ.get('CH_PORT', 8124))
CH_USER = os.environ.get('CH_USER', 'rag_user')
CH_PASSWORD = os.environ.get('CH_PASSWORD', '')
CH_DATABASE = os.environ.get('CH_DATABASE', 'rag')

def get_schema():
    client = clickhouse_connect.get_client(
        host=CH_HOST,
        port=CH_PORT,
        username=CH_USER,
        password=CH_PASSWORD,
        database=CH_DATABASE,
        secure=False,
        verify=False
    )
    
    print("--- NEW COLUMNS VERIFICATION ---")
    query = """
    SELECT 
        all_country_codes, 
        source_type, 
        sdg_ids, 
        awards, 
        concept_ids 
    FROM works 
    WHERE length(all_country_codes) > 0 
    LIMIT 3
    """
    res = client.query(query)
    for row in res.result_rows:
        print(row)

if __name__ == "__main__":
    get_schema()
