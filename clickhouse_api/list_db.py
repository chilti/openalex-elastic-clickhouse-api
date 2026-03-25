import os
import clickhouse_connect
from dotenv import load_dotenv

# Load from existing .env in same directory
env_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(env_path)

CH_HOST = os.environ.get('CH_HOST', '10.90.0.87')
CH_PORT = int(os.environ.get('CH_PORT', 8124))
CH_USER = os.environ.get('CH_USER', 'default')
CH_PASSWORD = os.environ.get('CH_PASSWORD', '')
CH_DATABASE = os.environ.get('CH_DATABASE', 'rag')

try:
    is_secure = (CH_PORT == 8124)
    client = clickhouse_connect.get_client(
        host=CH_HOST, 
        port=CH_PORT, 
        username=CH_USER,
        password=CH_PASSWORD,
        database=CH_DATABASE,
        secure=is_secure,
        verify=False
    )
    print(f"Databases in {CH_HOST}:", client.query("SHOW DATABASES").result_rows)
except Exception as e:
    print(f"Failed: {e}")
