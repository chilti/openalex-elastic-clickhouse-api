#!/usr/bin/env python3
"""
init_openalex_schemas.py
------------------------
Script para inicializar los esquemas de tablas de OpenAlex en ClickHouse.
Lee las definiciones DDL de openalex_schemas.sql y las ejecuta en el servidor
configurado en clickhouse_api/.env.

Uso:
    /home/ambientesPy/revistaslatam/bin/python init_openalex_schemas.py [--sql-file openalex_schemas.sql]
"""

import os
import sys
import argparse
import logging
from pathlib import Path
from dotenv import load_dotenv

try:
    import clickhouse_connect
except ImportError:
    print("Error: Se requiere clickhouse-connect. Instálalo o usa el entorno /home/ambientesPy/revistaslatam", file=sys.stderr)
    sys.exit(1)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("init_schemas")

def parse_sql_statements(sql_text: str):
    """Separa el archivo SQL en sentencias individuales respetando bloques y comentarios."""
    statements = []
    current = []
    for line in sql_text.splitlines():
        clean_line = line.strip()
        if not clean_line or clean_line.startswith("--"):
            continue
        current.append(line)
        if clean_line.endswith(";"):
            stmt = "\n".join(current).strip()
            if stmt.endswith(";"):
                stmt = stmt[:-1].strip()
            if stmt:
                statements.append(stmt)
            current = []
    if current:
        stmt = "\n".join(current).strip()
        if stmt:
            statements.append(stmt)
    return statements

def main():
    parser = argparse.ArgumentParser(description="Inicializar esquemas de OpenAlex en ClickHouse")
    parser.add_argument("--env-file", default=None, help="Ruta a archivo .env (default: clickhouse_api/.env)")
    parser.add_argument("--sql-file", default=None, help="Ruta al archivo SQL con los esquemas")
    args = parser.parse_args()

    # 1. Cargar .env
    default_env = Path(__file__).resolve().parent / ".env"
    env_path = Path(args.env_file) if args.env_file else default_env
    if env_path.exists():
        load_dotenv(env_path)
        logger.info(f"Cargadas variables de entorno desde: {env_path}")
    else:
        logger.warning(f"No se encontró archivo .env en {env_path}. Usando valores por defecto o variables de entorno.")

    ch_host = os.environ.get("CH_HOST", "localhost")
    ch_port = int(os.environ.get("CH_PORT", 8124))
    ch_user = os.environ.get("CH_USER", "default")
    ch_password = os.environ.get("CH_PASSWORD", "")
    ch_database = os.environ.get("CH_DATABASE", "rag")

    # 2. Localizar archivo SQL
    default_sql = Path(__file__).resolve().parent / "openalex_schemas.sql"
    sql_path = Path(args.sql_file) if args.sql_file else default_sql
    if not sql_path.exists():
        logger.error(f"No se encontró el archivo SQL en: {sql_path}")
        sys.exit(1)

    # 3. Conectar a ClickHouse
    logger.info(f"Conectando a ClickHouse en {ch_host}:{ch_port} (Base de datos: {ch_database})...")
    try:
        client = clickhouse_connect.get_client(
            host=ch_host,
            port=ch_port,
            username=ch_user,
            password=ch_password,
            database=ch_database,
            secure=False,
            verify=False,
            connect_timeout=20,
            send_receive_timeout=120,
        )
        logger.info("✅ Conexión establecida exitosamente.")
    except Exception as e:
        logger.error(f"❌ Error al conectar a ClickHouse: {e}")
        sys.exit(1)

    # 4. Leer y ejecutar sentencias DDL
    with open(sql_path, "r", encoding="utf-8") as f:
        sql_content = f.read()

    statements = parse_sql_statements(sql_content)
    logger.info(f"Ejecutando {len(statements)} sentencias DDL...")

    success_count = 0
    for i, stmt in enumerate(statements, 1):
        # Extraer nombre de tabla para el log
        first_line = stmt.strip().splitlines()[0]
        try:
            client.command(stmt)
            logger.info(f"  [{i}/{len(statements)}] OK: {first_line[:65]}...")
            success_count += 1
        except Exception as e:
            logger.error(f"  [{i}/{len(statements)}] Error en:\n{first_line}\nDetalle: {e}")

    # 5. Listar tablas resultantes en la base de datos
    try:
        res = client.query(f"SHOW TABLES FROM `{ch_database}`")
        tables = [r[0] for r in res.result_rows]
        logger.info("=" * 60)
        logger.info(f"📊 Tablas actualmente disponibles en '{ch_database}' ({len(tables)} tablas):")
        logger.info("=" * 60)
        for t in sorted(tables):
            logger.info(f"  - {t}")
    except Exception as e:
        logger.warning(f"No se pudo consultar lista de tablas: {e}")

    logger.info(f"Proceso finalizado: {success_count}/{len(statements)} sentencias aplicadas.")

if __name__ == "__main__":
    main()
