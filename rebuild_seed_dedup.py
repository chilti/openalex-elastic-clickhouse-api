"""
Rebuilds works_seed_mexico deduplicado usando argMax(col, updated_date) GROUP BY id.
Sin mutations pesadas — solo CREATE TABLE + INSERT + RENAME (atómico).

Uso:
    python rebuild_seed_dedup.py [--dry-run]
"""
import sys
import os
import clickhouse_connect
from dotenv import load_dotenv

load_dotenv('clickhouse_api/.env')

CH_HOST = os.getenv("CH_HOST", "localhost")
CH_PORT = int(os.getenv("CH_PORT", 8124))
CH_USER = os.getenv("CH_USER", "default")
CH_PASSWORD = os.getenv("CH_PASSWORD", "")
CH_DB = os.getenv("CH_DATABASE", "rag")

DRY_RUN = '--dry-run' in sys.argv

def get_client():
    return clickhouse_connect.get_client(
        host=CH_HOST, 
        port=CH_PORT, 
        username=CH_USER, 
        password=CH_PASSWORD, 
        database=CH_DB
    )

def main():
    client = get_client()

    # 1. Estado actual
    r = client.query("SELECT count(), count(DISTINCT id) FROM rag.works_seed_mexico")
    total, uniq = r.result_rows[0]
    print(f"works_seed_mexico actual — filas: {total:,}  únicos: {uniq:,}  dupes: {total-uniq:,}")

    if total == uniq:
        print("✅ La tabla ya está deduplicada. Nada que hacer.")
        return

    # 2. Obtener columnas en orden
    cols_r = client.query(
        "SELECT name, type FROM system.columns "
        "WHERE database='rag' AND table='works_seed_mexico' ORDER BY position"
    )
    cols = [(row[0], row[1]) for row in cols_r.result_rows]
    print(f"\n{len(cols)} columnas detectadas.")

    # 3. Construir SELECT con argMax para cada columna (excepto id que es GROUP BY key)
    # updated_date se usa como versión; para colecciones (Array) también argMax funciona.
    select_parts = []
    for name, typ in cols:
        if name == 'id':
            select_parts.append('id')
        elif name == 'updated_date':
            # La key de versión: tomamos la mayor (string ISO, ordenable lexicográficamente)
            select_parts.append(f"max(updated_date) AS updated_date")
        else:
            select_parts.append(f"argMax({name}, updated_date) AS {name}")

    col_names   = [c[0] for c in cols]
    select_sql  = ',\n    '.join(select_parts)

    create_sql = f"""
CREATE TABLE IF NOT EXISTS rag.works_seed_mexico_dedup
ENGINE = MergeTree()
ORDER BY (publication_year, id)
AS
SELECT
    {select_sql}
FROM rag.works_seed_mexico
GROUP BY id
"""

    rename_sql = """
RENAME TABLE
    rag.works_seed_mexico     TO rag.works_seed_mexico_old,
    rag.works_seed_mexico_dedup TO rag.works_seed_mexico
"""
    drop_sql = "DROP TABLE IF EXISTS rag.works_seed_mexico_old"

    if DRY_RUN:
        print("\n[dry-run] CREATE TABLE:\n", create_sql[:500], "...")
        print("[dry-run] RENAME + DROP listos. No se ejecutó nada.")
        return

    print("\n⏳ Creando works_seed_mexico_dedup con GROUP BY id...")
    print("   (esto puede tardar ~1-2 min para 350k filas)")
    client.command(create_sql, settings={'max_execution_time': 300})

    # Verificar resultado
    r2 = client.query("SELECT count(), count(DISTINCT id) FROM rag.works_seed_mexico_dedup")
    new_total, new_uniq = r2.result_rows[0]
    print(f"  → works_seed_mexico_dedup: {new_total:,} filas  ({new_uniq:,} únicos)")

    if new_total != new_uniq:
        print("⚠️  Todavía hay duplicados en la nueva tabla. Abortando rename.")
        client.command("DROP TABLE IF EXISTS rag.works_seed_mexico_dedup")
        return

    print("\n⏳ Intercambiando tablas (RENAME atómico)...")
    client.command(rename_sql)
    print("  → Rename completado.")

    print("\n⏳ Eliminando tabla antigua...")
    client.command(drop_sql)
    print("  → Tabla antigua eliminada.")

    r3 = client.query("SELECT count() FROM rag.works_seed_mexico")
    print(f"\n✅ works_seed_mexico final: {r3.result_rows[0][0]:,} filas únicas.")

if __name__ == '__main__':
    main()
