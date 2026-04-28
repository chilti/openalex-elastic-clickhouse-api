import clickhouse_connect
import os
from dotenv import load_dotenv

# Cargar configuración
load_dotenv('clickhouse_api/.env')

CH_HOST = os.environ.get('CH_HOST', 'localhost')
CH_PORT = int(os.environ.get('CH_PORT', 8124))
CH_USER = os.environ.get('CH_USER', 'default')
CH_PASSWORD = os.environ.get('CH_PASSWORD', '')
CH_DATABASE = os.environ.get('CH_DATABASE', 'rag')

def materialize_fields():
    client = clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASSWORD, 
        database=CH_DATABASE, secure=False
    )
    
    print(f"Conectado a {CH_HOST}. Iniciando materialización en tabla 'works'...")

    commands = [
        # 1. SDGs
        "ALTER TABLE works ADD COLUMN IF NOT EXISTS sdg_ids Array(String) MATERIALIZED JSONExtract(raw_data, 'sustainable_development_goals', 'Array(Tuple(id String))').id",
        
        # 2. Todos los Topics (Array)
        "ALTER TABLE works ADD COLUMN IF NOT EXISTS topic_ids Array(String) MATERIALIZED JSONExtract(raw_data, 'topics', 'Array(Tuple(id String))').id",
        
        # 3. Jerarquía del Tópico Primario
        "ALTER TABLE works ADD COLUMN IF NOT EXISTS primary_subfield_id String MATERIALIZED JSONExtractString(raw_data, 'primary_topic', 'subfield', 'id')",
        "ALTER TABLE works ADD COLUMN IF NOT EXISTS primary_field_id String MATERIALIZED JSONExtractString(raw_data, 'primary_topic', 'field', 'id')",
        "ALTER TABLE works ADD COLUMN IF NOT EXISTS primary_domain_id String MATERIALIZED JSONExtractString(raw_data, 'primary_topic', 'domain', 'id')",
        
        # Comandos para forzar el cálculo en datos existentes (esto puede tardar, se hace en background)
        "ALTER TABLE works MATERIALIZE COLUMN sdg_ids",
        "ALTER TABLE works MATERIALIZE COLUMN topic_ids",
        "ALTER TABLE works MATERIALIZE COLUMN primary_subfield_id",
        "ALTER TABLE works MATERIALIZE COLUMN primary_field_id",
        "ALTER TABLE works MATERIALIZE COLUMN primary_domain_id"
    ]

    for cmd in commands:
        try:
            print(f"Ejecutando: {cmd[:60]}...")
            client.command(cmd)
            print("OK.")
        except Exception as e:
            print(f"Error o ya existía: {e}")

    print("\n¡Materialización iniciada! ClickHouse procesará los datos existentes en background.")

if __name__ == "__main__":
    materialize_fields()
