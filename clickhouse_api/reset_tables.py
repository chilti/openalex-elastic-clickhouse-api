"""
reset_tables.py
---------------
Limpia las tablas de OpenAlex en ClickHouse para permitir una recarga
completa y limpia desde el snapshot.

IMPORTANTE: Este script hace TRUNCATE de las tablas raw. Es destructivo.
Pide confirmación explícita antes de proceder.

Uso:
    python clickhouse_api/reset_tables.py
"""

import os
import sys
import time
import clickhouse_connect
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

CH_HOST     = os.environ.get("CH_HOST", "10.90.0.87")
CH_PORT     = int(os.environ.get("CH_PORT", 8124))
CH_USER     = os.environ.get("CH_USER", "rag_user")
CH_PASSWORD = os.environ.get("CH_PASSWORD", "")
CH_DATABASE = os.environ.get("CH_DATABASE", "rag")

# ─────────────────────────────────────────────────────────────────────────────
# Tablas raw del snapshot (se vacían y recargan desde el snapshot de OpenAlex)
# El orden importa: primero las derivadas, luego las raw
# ─────────────────────────────────────────────────────────────────────────────
TABLES_TO_TRUNCATE = [
    # — Tablas derivadas / agregadas (dependen de works) —
    ("summing_subfield_metrics",       "SummingMergeTree — se reconstruye con create_subfield_mv.py"),
    ("summing_subfield_inst_metrics",  "SummingMergeTree — se reconstruye con create_subfield_mv.py"),
    ("works_flat",                     "ReplacingMergeTree — se reconstruye con migrate_to_flat_table.py"),
    # — Tablas raw del snapshot —
    ("works",          "MergeTree raw — snapshot: 2,127 archivos, 595 GiB"),
    ("authors",        "MergeTree raw — snapshot: 546 archivos, 65 GiB"),
    ("awards",         "MergeTree raw — snapshot: 20 archivos, 2.8 GiB"),
    ("institutions",   "MergeTree raw — snapshot: 50 archivos, 169 MiB"),
    ("sources",        "MergeTree raw — snapshot: 39 archivos, 330 MiB"),
    ("funders",        "ReplacingMergeTree raw — snapshot: 45 archivos"),
    ("concepts",       "ReplacingMergeTree raw — snapshot: 19 archivos"),
    ("topics",         "ReplacingMergeTree raw — snapshot: 4 archivos"),
    ("keywords",       "ReplacingMergeTree raw — snapshot: 28 archivos"),
    ("publishers",     "ReplacingMergeTree raw — snapshot: 46 archivos"),
    ("subfields",      "MergeTree raw — snapshot: 1 archivo"),
    ("countries",      "ReplacingMergeTree raw — snapshot: 6 archivos"),
    ("domains",        "ReplacingMergeTree raw — snapshot: 1 archivo"),
    ("fields",         "ReplacingMergeTree raw — snapshot: 1 archivo"),
    ("languages",      "ReplacingMergeTree raw — snapshot: 21 archivos"),
    ("licenses",       "ReplacingMergeTree raw — snapshot: 2 archivos"),
    ("sdgs",           "ReplacingMergeTree raw — snapshot: 1 archivo"),
    ("source-types",   "ReplacingMergeTree raw — snapshot: 1 archivo"),
    ("institution-types", "ReplacingMergeTree raw — snapshot: 1 archivo"),
    ("work-types",     "ReplacingMergeTree raw — snapshot: 3 archivos"),
    # — Control —
    ("_processed_files", "MergeTree — registro de archivos ya procesados"),
]

# Tablas que NO se tocan (datos propios, no del snapshot)
TABLES_PRESERVED = [
    "continents",              # OK según diagnóstico
    "works_seed_mexico",       # semilla propia
    "authors_seed_mexico",     # semilla propia
    "institutions_seed_mexico",# semilla propia
    "academics_all",           # datos propios
    "works_academic_all",      # datos propios
    "awards",                  # si resulta ser dato propio, comentar de TABLES_TO_TRUNCATE
    "paper_author_map",        # mapa propio
    "paper_entity_map",        # mapa propio
    "embeddings_cache",        # caché de embeddings
]

# ─────────────────────────────────────────────────────────────────────────────

def get_client():
    return clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT,
        username=CH_USER, password=CH_PASSWORD,
        database=CH_DATABASE,
        secure=False, verify=False,
        connect_timeout=30,
        send_receive_timeout=600,  # TRUNCATEs en tablas grandes pueden tardar
    )


def show_current_counts(client):
    print(f"\n{'─'*65}")
    print("  Estado actual de las tablas (antes del reset):")
    print(f"{'─'*65}")
    for table, desc in TABLES_TO_TRUNCATE:
        try:
            res = client.query(f"SELECT count() FROM `{CH_DATABASE}`.`{table}`")
            count = res.first_row[0]
            print(f"  {table:<35} {count:>15,}  filas")
        except Exception as e:
            print(f"  {table:<35} ERROR: {str(e)[:40]}")
    print(f"{'─'*65}\n")


def detach_mv(client):
    """Desconecta la Materialized View para evitar escrituras dobles durante la recarga."""
    print("  Desconectando works_flat_mv (MV)...")
    try:
        client.command("DETACH TABLE IF EXISTS `rag`.`works_flat_mv`")
        print("  ✅ works_flat_mv desconectada")
    except Exception as e:
        print(f"  ⚠️  No se pudo desconectar works_flat_mv: {e}")


def attach_mv(client):
    """Re-conecta la Materialized View."""
    print("  Re-conectando works_flat_mv...")
    try:
        client.command("ATTACH TABLE `rag`.`works_flat_mv`")
        print("  ✅ works_flat_mv reconectada")
    except Exception as e:
        print(f"  ⚠️  No se pudo reconectar works_flat_mv: {e}")


def do_truncate(client):
    """Ejecuta TRUNCATE en todas las tablas de la lista con pausas entre operaciones."""
    print(f"\n{'─'*65}")
    print(f"  Iniciando TRUNCATE — {datetime.now():%Y-%m-%d %H:%M:%S}")
    print(f"{'─'*65}")
    errors = []
    for i, (table, desc) in enumerate(TABLES_TO_TRUNCATE):
        try:
            client.command(
                f"TRUNCATE TABLE `{CH_DATABASE}`.`{table}` "
                f"SETTINGS max_table_size_to_drop = 0"
            )
            print(f"  ✅ TRUNCATE {table:<35} — OK")
        except Exception as e:
            msg = str(e)[:80]
            print(f"  ❌ TRUNCATE {table:<35} — ERROR: {msg}")
            errors.append((table, msg))

        # Pausa entre operaciones para no saturar el servidor
        # Las primeras tablas (derivadas grandes) necesitan más tiempo
        pause = 3.0 if i < 3 else 1.0
        time.sleep(pause)

    return errors


def main():
    print(f"\n{'='*65}")
    print("  RESET DE TABLAS OPENALEX EN CLICKHOUSE")
    print(f"  {CH_HOST}:{CH_PORT} / {CH_DATABASE}")
    print(f"{'='*65}")

    client = get_client()
    print("\n  ✅ Conexión a ClickHouse establecida")

    # Mostrar estado actual
    show_current_counts(client)

    # Advertencia clara
    print("  ⚠️  ADVERTENCIA: Este script hará TRUNCATE de las siguientes tablas:")
    for table, _ in TABLES_TO_TRUNCATE:
        print(f"      • {table}")
    print()
    print("  Las siguientes tablas NO se tocarán (datos propios):")
    for t in TABLES_PRESERVED:
        print(f"      • {t}")
    print()
    print("  Después del reset debes ejecutar la recarga:")
    print("      python clickhouse_api/load_openalex_clickhouse.py <path_snapshot_data> --workers 8")
    print()

    # Confirmación
    answer = input("  ¿Confirmas el TRUNCATE de todas las tablas listadas? [escribe 'SI' para continuar]: ").strip()
    if answer.upper() != "SI":
        print("\n  ❌ Operación cancelada.\n")
        sys.exit(0)

    print()

    # 1. Desconectar MV
    detach_mv(client)

    # 2. TRUNCATE
    errors = do_truncate(client)

    # Nota: NO reconectamos la MV aquí. Debe permanecer desconectada
    # durante toda la carga de OpenAlex para evitar errores TOO_MANY_PARTS.
    # Se reconectará después de ejecutar migrate_to_flat_table.py

    # Resumen
    print(f"\n{'='*65}")
    if errors:
        print(f"  ⚠️  Reset completado CON {len(errors)} errores:")
        for t, e in errors:
            print(f"      • {t}: {e}")
    else:
        print("  🎉 Reset completado exitosamente. Todas las tablas vaciadas.")

    print()
    print("  SIGUIENTES PASOS:")
    print("  1. SSH al servidor de datos")
    print("  2. Ejecutar recarga desde snapshot")
    print()
    print("  3. Cuando termine la recarga, ejecutar desde aquí:")
    print("         python scripts/migrate_to_flat_table.py")
    print()
    print("  4. Reconstruir tablas derivadas:")
    print("         python scripts/create_subfield_mv.py")
    print(f"{'='*65}\n")


if __name__ == "__main__":
    main()
