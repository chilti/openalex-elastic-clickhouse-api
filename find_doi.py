
import clickhouse_connect
import os
from dotenv import load_dotenv

load_dotenv(r'c:\Users\jlja\Documents\Proyectos\RAGs\.env')

client = clickhouse_connect.get_client(
    host=os.getenv('CH_HOST'), 
    port=int(os.getenv('CH_PORT', 8124)), 
    username=os.getenv('CH_USER'), 
    password=os.getenv('CH_PASSWORD')
)

doi = '10.1103/physrevlett.123.011101'
print(f"Buscando DOI: {doi}")

# 1. Probar en rag.works
try:
    count = client.query(f"SELECT count() FROM rag.works WHERE doi LIKE '%{doi}%'").result_rows[0][0]
    print(f"Resultados en rag.works: {count}")
except Exception as e:
    print(f"Error en rag.works: {e}")

# 2. Listar otras bases de datos y buscar tablas 'works'
try:
    dbs = client.query("SHOW DATABASES").result_rows
    for db_row in dbs:
        db = db_row[0]
        if db in ['system', 'information_schema']: continue
        tables = client.query(f"SHOW TABLES FROM {db}").result_rows
        for table_row in tables:
            table = table_row[0]
            if 'works' in table.lower():
                try:
                    c = client.query(f"SELECT count() FROM {db}.{table} WHERE doi LIKE '%{doi}%'").result_rows[0][0]
                    if c > 0:
                        print(f"¡ENCONTRADO! en {db}.{table}: {c}")
                except:
                    pass
except Exception as e:
    print(f"Error explorando DBs: {e}")
