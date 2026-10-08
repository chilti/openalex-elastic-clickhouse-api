import os
import clickhouse_connect
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

CH_HOST = os.environ.get('CH_HOST', 'localhost')
CH_PORT = int(os.environ.get('CH_PORT', 8123))
CH_USER = os.environ.get('CH_USER', 'default')
CH_PASSWORD = os.environ.get('CH_PASSWORD', '')
CH_DATABASE = os.environ.get('CH_DATABASE', 'rag')

def check_status():
    print(f"📊 Conectando a ClickHouse en {CH_HOST}:{CH_PORT} (BD: {CH_DATABASE})...")
    
    try:
        client = clickhouse_connect.get_client(
            host=CH_HOST,
            port=CH_PORT,
            username=CH_USER,
            password=CH_PASSWORD,
            database=CH_DATABASE
        )
        
        # 1. Consultar tablas de OpenAlex y base de datos
        tables = [t[0] for t in client.query("SHOW TABLES").result_rows]
        
        # Lista de entidades esperadas de OpenAlex
        OPENALEX_ENTITIES = ['works', 'authors', 'sources', 'institutions', 'concepts', 'venues', 'publishers', 'funders', 'topics', 'data', 'legacy-data']
        
        print("\n" + "="*50)
        print(f"{'TABLA':<30} | {'REGISTROS':>15}")
        print("-" * 50)
        
        total_records = 0
        for table_name in tables:
            # Detectar si es una tabla de datos (por nombre o prefijo legacy)
            is_data_table = table_name in OPENALEX_ENTITIES or table_name.startswith('openalex_')
            
            if is_data_table or table_name == '_processed_files':
                count = client.command(f"SELECT count() FROM `{table_name}`")
                print(f"{table_name:<30} | {count:>15,}")
                if is_data_table:
                    total_records += count
                    
        print("-" * 50)
        print(f"{'TOTAL ENTIDADES':<30} | {total_records:>15,}")
        print("="*50)
        
        # 2. Resumen de archivos procesados
        if ('_processed_files',) in tables:
            processed = client.query("SELECT entity, count() FROM _processed_files GROUP BY entity").result_rows
            print("\n📂 Archivos .gz procesados por entidad:")
            for entity, file_count in processed:
                print(f"  - {entity}: {file_count} archivos")
                
    except Exception as e:
        print(f"❌ Error al conectar o consultar ClickHouse: {e}")

if __name__ == "__main__":
    check_status()
