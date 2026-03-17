import os
import logging
import argparse
import json
import gzip
from pathlib import Path
from dotenv import load_dotenv
from concurrent.futures import ProcessPoolExecutor, as_completed

# Cargar variables de entorno desde .env si existe
load_dotenv()

# Se requiere tener instalado clickhouse-connect
try:
    import clickhouse_connect
except ImportError:
    print("Por favor instala clickhouse-connect: pip install clickhouse-connect")
    exit(1)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuración de ClickHouse
CH_HOST = os.environ.get('CH_HOST', 'localhost')
CH_PORT = int(os.environ.get('CH_PORT', 8123))
CH_USER = os.environ.get('CH_USER', 'default')
CH_PASSWORD = os.environ.get('CH_PASSWORD', '')
CH_DATABASE = os.environ.get('CH_DATABASE', 'rag')

def get_clickhouse_client():
    """Conecta al servidor ClickHouse y asegura la base de datos."""
    client = clickhouse_connect.get_client(
        host=CH_HOST, 
        port=CH_PORT, 
        username=CH_USER, 
        password=CH_PASSWORD,
        database=CH_DATABASE
    )
    return client

def ensure_base_tables(client):
    """Crea la tabla de control de archivos procesados."""
    client.command(f"""
        CREATE TABLE IF NOT EXISTS _processed_files (
            entity String,
            file_name String,
            processed_at DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        ORDER BY (entity, file_name)
    """)

def discover_entities(snapshot_path: Path):
    """Escanea la raíz del snapshot y descubre qué entidades existen."""
    entities = []
    if not snapshot_path.exists() or not snapshot_path.is_dir():
        return entities
        
    for item in snapshot_path.iterdir():
        if item.is_dir() and not item.name.startswith('.'):
            files = list(item.glob('**/*.gz'))
            if len(files) > 0:
                entities.append(item.name)
    return entities

def infer_and_create_schema(client, entity_name: str):
    """Crea la tabla para los documentos JSON crudos."""
    table_name = f"`{entity_name}`"
    create_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id String,
        raw_data String
    ) ENGINE = ReplacingMergeTree()
    ORDER BY id
    """
    client.command(create_query)

def process_single_file(file_path: Path, entity_name: str, entity_dir: Path):
    """Procesa un solo archivo .gz e inserta los datos en ClickHouse."""
    try:
        client = get_clickhouse_client()
        relative_path = str(file_path.relative_to(entity_dir))
        table_name = f"`{entity_name}`"
        
        rows = []
        with gzip.open(file_path, 'rt', encoding='utf-8') as f:
            for line in f:
                if not line.strip(): continue
                try:
                    obj = json.loads(line)
                    rows.append([obj.get('id', ''), line.strip()])
                except: continue
                
                if len(rows) >= 10000: # Aumentado el tamaño del batch para eficiencia
                    client.insert(table_name, rows, column_names=['id', 'raw_data'])
                    rows = []
        
        if rows:
            client.insert(table_name, rows, column_names=['id', 'raw_data'])
        
        # Registrar archivo como completado
        client.command(
            f"INSERT INTO _processed_files (entity, file_name) VALUES ('{entity_name}', '{relative_path}')"
        )
        return True, relative_path
    except Exception as e:
        return False, f"{file_path.name}: {str(e)}"

def ingest_entity(entity_name: str, snapshot_path: Path, max_workers: int):
    """Gestiona la ingesta de una entidad en paralelo."""
    entity_dir = snapshot_path / entity_name
    files = list(entity_dir.glob('**/*.gz'))
    
    if not files:
        logger.warning(f"[{entity_name}] No se encontraron archivos .gz")
        return

    client = get_clickhouse_client()
    ensure_base_tables(client)
    infer_and_create_schema(client, entity_name)
    
    # Obtener archivos ya procesados
    result = client.query(f"SELECT file_name FROM _processed_files WHERE entity = '{entity_name}'")
    processed_files = set(result.result_columns[0]) if result.result_columns else set()
    
    files_to_process = []
    for f in files:
        rel = str(f.relative_to(entity_dir))
        if rel not in processed_files and f.name not in processed_files:
            files_to_process.append(f)

    if not files_to_process:
        logger.info(f"[{entity_name}] Todos los archivos ({len(files)}) ya fueron procesados.")
        return

    logger.info(f"[{entity_name}] Iniciando ingesta paralela de {len(files_to_process)} archivos con {max_workers} workers...")
    
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_single_file, f, entity_name, entity_dir): f for f in files_to_process}
        
        completed = 0
        for future in as_completed(futures):
            success, info = future.result()
            completed += 1
            if success:
                if completed % 10 == 0 or completed == len(files_to_process):
                    logger.info(f"  -> [{entity_name}] {completed}/{len(files_to_process)} archivos procesados.")
            else:
                logger.error(f"  ❌ Error en {info}")

def main():
    parser = argparse.ArgumentParser(description="Ingesta paralela de OpenAlex a ClickHouse.")
    parser.add_argument("snapshot_dir", help="Directorio raíz del snapshot.")
    parser.add_argument("--entities", nargs="+", help="Lista de entidades a procesar.")
    parser.add_argument("--workers", type=int, default=os.cpu_count() or 4, help="Número de procesos paralelos.")
    args = parser.parse_args()
    
    snapshot_path = Path(args.snapshot_dir)
    client = get_clickhouse_client()
    ensure_base_tables(client)
    
    entities_to_process = args.entities or discover_entities(snapshot_path)
    
    if not entities_to_process:
        logger.error("No hay entidades a procesar.")
        return

    for entity in entities_to_process:
        ingest_entity(entity, snapshot_path, args.workers)
        
    logger.info("🎉 Ingesta Global Finalizada.")

if __name__ == "__main__":
    main()
