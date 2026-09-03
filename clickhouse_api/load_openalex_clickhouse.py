import os
import sys
import time
import logging
import argparse
import json
import gzip
from pathlib import Path
from dotenv import load_dotenv
from concurrent.futures import ProcessPoolExecutor, as_completed

# Cargar variables de entorno (se puede sobreescribir con --env-file)
# load_dotenv() se llama después de parsear --env-file en main()

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
    """Conecta al servidor ClickHouse con timeouts conservadores."""
    client = clickhouse_connect.get_client(
        host=CH_HOST,
        port=CH_PORT,
        username=CH_USER,
        password=CH_PASSWORD,
        database=CH_DATABASE,
        # Timeouts generosos para no matar conexiones en medio de inserts grandes
        connect_timeout=30,
        send_receive_timeout=600,   # 10 min máximo por operación
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

def process_single_file(
    file_path: Path,
    entity_name: str,
    entity_dir: Path,
    batch_size: int = 5000,
    delay: float = 0.0,
    max_retries: int = 3,
):
    """
    Procesa un solo archivo .gz e inserta los datos en ClickHouse.

    Incluye reintentos con backoff exponencial para no saturar el servidor.
    `delay` es un sleep (segundos) entre cada batch de inserts.
    """
    relative_path = str(file_path.relative_to(entity_dir))
    table_name    = f"`{entity_name}`"

    for attempt in range(1, max_retries + 1):
        try:
            client = get_clickhouse_client()
            rows = []

            with gzip.open(file_path, 'rt', encoding='utf-8') as f:
                for line in f:
                    if not line.strip():
                        continue
                    try:
                        obj = json.loads(line)
                        rows.append([obj.get('id', ''), line.strip()])
                    except Exception:
                        continue

                    if len(rows) >= batch_size:
                        client.insert(table_name, rows, column_names=['id', 'raw_data'])
                        rows = []
                        if delay > 0:
                            time.sleep(delay)  # pausa entre batches

            if rows:
                client.insert(table_name, rows, column_names=['id', 'raw_data'])

            # Registrar archivo como completado
            rel_escaped = relative_path.replace("'", "''")
            client.command(
                f"INSERT INTO _processed_files (entity, file_name) "
                f"VALUES ('{entity_name}', '{rel_escaped}')"
            )
            return True, relative_path

        except Exception as e:
            wait = 10 * (2 ** (attempt - 1))  # 10s, 20s, 40s
            if attempt < max_retries:
                # Log al stderr porque estamos en un subproceso
                import sys
                print(
                    f"[{entity_name}] ⚠️  Intento {attempt}/{max_retries} fallido "
                    f"para {file_path.name}: {e}. Reintentando en {wait}s...",
                    file=sys.stderr
                )
                time.sleep(wait)
            else:
                return False, f"{file_path.name}: {str(e)} (fallido tras {max_retries} intentos)"

def reconcile_entity(client, entity_name: str, entity_dir: Path, disk_files: set) -> bool:
    """
    Detecta si algún archivo registrado en _processed_files ya no existe en el
    snapshot (borrado por aws s3 sync --delete u otro proceso).

    Si detecta archivos fantasma:
      1. TRUNCATE de la tabla de la entidad.
      2. Limpia los registros de _processed_files para esa entidad.
      3. Devuelve True para forzar una recarga completa.

    Si todo está consistente devuelve False (sin cambios).
    """
    result = client.query(
        f"SELECT file_name FROM _processed_files WHERE entity = '{entity_name}'"
    )
    registered = set(result.result_columns[0]) if result.result_columns else set()

    if not registered:
        return False  # Nunca se procesó, recarga normal

    # Archivos registrados en CH que ya no existen en disco
    deleted = registered - disk_files

    if not deleted:
        logger.info(f"[{entity_name}] Reconciliación OK — ningún archivo fue borrado del snapshot.")
        return False

    logger.warning(
        f"[{entity_name}] ⚠️  Se detectaron {len(deleted)} archivo(s) borrados del snapshot "
        f"pero aún registrados en _processed_files. Ejemplos: {list(deleted)[:3]}"
    )
    logger.warning(f"[{entity_name}] → Haciendo TRUNCATE de la tabla y recargando desde cero...")

    # TRUNCATE de la tabla raw de la entidad
    try:
        client.command(
            f"TRUNCATE TABLE `{CH_DATABASE}`.`{entity_name}` "
            f"SETTINGS max_table_size_to_drop = 0"
        )
        logger.info(f"[{entity_name}] ✅ TRUNCATE completado.")
    except Exception as e:
        logger.error(f"[{entity_name}] ❌ Error en TRUNCATE: {e}")
        raise

    # Limpiar _processed_files para esta entidad
    client.command(
        f"ALTER TABLE _processed_files DELETE WHERE entity = '{entity_name}'"
    )
    logger.info(f"[{entity_name}] ✅ _processed_files limpiado para esta entidad.")
    return True  # Forzar recarga completa


def ingest_entity(
    entity_name: str,
    snapshot_path: Path,
    max_workers: int,
    reconcile: bool = False,
    batch_size: int = 5000,
    delay: float = 1.0,
    entity_pause: float = 5.0,
):
    """Gestiona la ingesta de una entidad en paralelo de forma controlada.

    Args:
        entity_name:  Nombre de la entidad (works, authors, etc.).
        snapshot_path: Ruta raíz del snapshot.
        max_workers:  Número máximo de procesos paralelos.
        reconcile:    Si True, detecta archivos borrados y resetea la entidad.
        batch_size:   Filas por batch de insert (default conservador: 5,000).
        delay:        Segundos de pausa entre batches de inserts dentro de un archivo.
        entity_pause: Segundos de pausa después de terminar esta entidad, para
                      dar tiempo a ClickHouse de hacer merge y liberar recursos.
    """
    entity_dir = snapshot_path / entity_name
    files = list(entity_dir.glob('**/*.gz'))

    if not files:
        logger.warning(f"[{entity_name}] No se encontraron archivos .gz")
        return

    client = get_clickhouse_client()
    ensure_base_tables(client)
    infer_and_create_schema(client, entity_name)

    # Conjunto de rutas relativas de los archivos actualmente en disco
    disk_files = {str(f.relative_to(entity_dir)) for f in files}

    # ── Reconciliación de archivos borrados ──────────────────────────────────
    if reconcile:
        reconcile_entity(client, entity_name, entity_dir, disk_files)

    # Obtener archivos ya procesados (puede estar vacío si reconcile hizo TRUNCATE)
    result = client.query(f"SELECT file_name FROM _processed_files WHERE entity = '{entity_name}'")
    processed_files = set(result.result_columns[0]) if result.result_columns else set()

    files_to_process = [
        f for f in files
        if str(f.relative_to(entity_dir)) not in processed_files
        and f.name not in processed_files
    ]

    if not files_to_process:
        logger.info(f"[{entity_name}] Todos los archivos ({len(files)}) ya fueron procesados.")
        return

    logger.info(
        f"[{entity_name}] Iniciando ingesta de {len(files_to_process)} archivos "
        f"| workers={max_workers} | batch={batch_size} | delay={delay}s"
    )

    # Enviar trabajos de a `max_workers` a la vez para no inundar CH
    # (en lugar de submitear todos y dejar que el executor compita)
    completed  = 0
    errors     = 0
    total      = len(files_to_process)

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        # Chunk: enviamos solo `max_workers` archivos en vuelo a la vez
        chunk_size = max_workers
        for chunk_start in range(0, total, chunk_size):
            chunk = files_to_process[chunk_start: chunk_start + chunk_size]
            futures = {
                executor.submit(
                    process_single_file, f, entity_name, entity_dir, batch_size, delay
                ): f
                for f in chunk
            }
            for future in as_completed(futures):
                success, info = future.result()
                completed += 1
                if success:
                    if completed % 10 == 0 or completed == total:
                        logger.info(
                            f"  -> [{entity_name}] {completed}/{total} "
                            f"({errors} errores hasta ahora)"
                        )
                else:
                    errors += 1
                    logger.error(f"  ❌ [{entity_name}] Error en {info}")

            # Pausa entre chunks para que CH procese los merges pendientes
            if chunk_start + chunk_size < total:
                logger.debug(f"  [{entity_name}] Pausa de {delay}s entre chunks...")
                time.sleep(delay)

    logger.info(
        f"[{entity_name}] ✅ Entidad completa: {completed - errors}/{total} archivos OK, "
        f"{errors} errores."
    )

    # Pausa post-entidad: dar tiempo al servidor para hacer merges
    if entity_pause > 0:
        logger.info(f"[{entity_name}] Pausa de {entity_pause}s antes de la siguiente entidad...")
        time.sleep(entity_pause)

def main():
    parser = argparse.ArgumentParser(
        description="Ingesta paralela de OpenAlex a ClickHouse.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Ejemplos:
  # Carga normal (solo archivos nuevos, configuración conservadora)
  python load_openalex_clickhouse.py openalex-snapshot/data

  # Más agresivo (más workers, sin delay) — solo si el servidor lo aguanta
  python load_openalex_clickhouse.py openalex-snapshot/data --workers 8 --delay 0

  # Con reconciliación (tras aws s3 sync --delete)
  python load_openalex_clickhouse.py openalex-snapshot/data --reconcile

  # Solo entidades específicas
  python load_openalex_clickhouse.py openalex-snapshot/data --entities works authors --reconcile
"""
    )
    parser.add_argument("snapshot_dir", help="Directorio raíz del snapshot (carpeta 'data').")
    parser.add_argument("--entities", nargs="+", help="Entidades a procesar. Por defecto: todas.")
    parser.add_argument(
        "--workers", type=int, default=4,
        help="Procesos paralelos (default: 4). Más workers = más presión en CH."
    )
    parser.add_argument(
        "--batch-size", type=int, default=5000,
        help="Filas por batch de INSERT (default: 5000). Más bajo = menos presión en CH."
    )
    parser.add_argument(
        "--delay", type=float, default=1.0,
        help="Segundos de pausa entre batches de INSERT y entre chunks de archivos (default: 1.0)."
    )
    parser.add_argument(
        "--entity-pause", type=float, default=5.0,
        help="Segundos de pausa después de cada entidad completa (default: 5.0)."
    )
    parser.add_argument(
        "--reconcile", action="store_true",
        help=(
            "Activa la reconciliación de archivos borrados. "
            "Compara los archivos del snapshot en disco contra _processed_files. "
            "Si detecta archivos que fueron eliminados del snapshot (ej. tras "
            "'aws s3 sync --delete'), hace TRUNCATE de esa entidad y la recarga "
            "desde cero. Úsalo siempre que el snapshot haya sido sincronizado con --delete."
        )
    )
    parser.add_argument(
        "--env-file", default=None,
        help="Ruta a un archivo .env alternativo (útil al ejecutar desde otro servidor)."
    )
    args = parser.parse_args()

    # Cargar variables de entorno
    env_path = args.env_file or os.path.join(os.path.dirname(__file__), ".env")
    load_dotenv(env_path)
    # Re-leer variables globales después de cargar .env
    global CH_HOST, CH_PORT, CH_USER, CH_PASSWORD, CH_DATABASE
    CH_HOST     = os.environ.get('CH_HOST', 'localhost')
    CH_PORT     = int(os.environ.get('CH_PORT', 8123))
    CH_USER     = os.environ.get('CH_USER', 'default')
    CH_PASSWORD = os.environ.get('CH_PASSWORD', '')
    CH_DATABASE = os.environ.get('CH_DATABASE', 'rag')

    snapshot_path = Path(args.snapshot_dir)
    if not snapshot_path.exists():
        logger.error(f"El directorio del snapshot no existe: {snapshot_path}")
        sys.exit(1)

    client = get_clickhouse_client()
    ensure_base_tables(client)

    entities_to_process = args.entities or discover_entities(snapshot_path)

    if not entities_to_process:
        logger.error("No hay entidades a procesar.")
        return

    logger.info(
        f"🚚 Configuración de carga: workers={args.workers} | "
        f"batch={args.batch_size} | delay={args.delay}s | entity_pause={args.entity_pause}s"
    )
    if args.reconcile:
        logger.info(
            "🔍 Modo --reconcile activo: se detectarán y resetearán entidades "
            "con archivos borrados del snapshot."
        )

    for entity in entities_to_process:
        ingest_entity(
            entity,
            snapshot_path,
            max_workers=args.workers,
            reconcile=args.reconcile,
            batch_size=args.batch_size,
            delay=args.delay,
            entity_pause=args.entity_pause,
        )

    logger.info("🎉 Ingesta Global Finalizada.")


if __name__ == "__main__":
    main()
