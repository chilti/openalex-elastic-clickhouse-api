"""
Script temporal para inspeccionar la estructura de la base de datos ClickHouse.
"""
import os
import sys
import clickhouse_connect
from dotenv import load_dotenv

load_dotenv()

CH_HOST = os.environ.get('CH_HOST', '10.90.0.87')
CH_PORT = int(os.environ.get('CH_PORT', 8124))
CH_USER = os.environ.get('CH_USER', 'rag_user')
CH_PASSWORD = os.environ.get('CH_PASSWORD', '')
CH_DATABASE = os.environ.get('CH_DATABASE', 'rag')

def get_client():
    # Puerto 8124 = HTTP sin SSL (no HTTPS)
    try:
        return clickhouse_connect.get_client(
            host=CH_HOST, port=CH_PORT,
            username=CH_USER, password=CH_PASSWORD,
            database=CH_DATABASE,
            secure=False, verify=False,
            connect_timeout=10
        )
    except Exception as e:
        print(f"Error conectando: {e}", file=sys.stderr)
        raise

client = get_client()

print("=" * 70)
print("1. TODAS LAS TABLAS Y VISTAS en 'rag'")
print("=" * 70)
res = client.query("""
    SELECT name, engine, total_rows, formatReadableSize(total_bytes) as size
    FROM system.tables
    WHERE database = 'rag'
    ORDER BY engine, name
""")
for r in res.result_rows:
    print(f"  [{r[1]:30s}] {r[0]:40s} rows={r[2]}  size={r[3]}")

print()
print("=" * 70)
print("2. VISTAS MATERIALIZADAS - detalle de fuentes y destinos")
print("=" * 70)
res2 = client.query("""
    SELECT name, engine, as_select
    FROM system.tables
    WHERE database = 'rag' AND engine LIKE '%MaterializedView%'
""")
for r in res2.result_rows:
    print(f"\n  MV: {r[0]}")
    # as_select puede ser largo, cortamos
    print(f"  SELECT (primeros 300 chars):\n    {str(r[2])[:300]}")

print()
print("=" * 70)
print("3. CONTEO DE REGISTROS EN TABLAS PRINCIPALES")
print("=" * 70)
main_tables = ['works', 'authors', 'institutions', 'sources', 'topics', 
               'concepts', 'publishers', 'funders',
               'works_flat', 'works_seed_mexico', 'authors_seed_mexico',
               'institutions_seed_mexico', '_processed_files',
               'summing_subfield_metrics']
for t in main_tables:
    try:
        r = client.query(f"SELECT count() FROM rag.`{t}`")
        print(f"  {t:45s}: {r.first_row[0]:>15,}")
    except Exception as e:
        print(f"  {t:45s}: ERROR - {str(e)[:60]}")

print()
print("=" * 70)
print("4. ESTADO DE _processed_files POR ENTIDAD")
print("=" * 70)
try:
    res3 = client.query("""
        SELECT entity, count() as n_files, min(processed_at), max(processed_at)
        FROM rag._processed_files
        GROUP BY entity
        ORDER BY entity
    """)
    for r in res3.result_rows:
        print(f"  {r[0]:20s}: {r[1]:6} archivos | desde {r[2]} hasta {r[3]}")
except Exception as e:
    print(f"  Error: {e}")

print()
print("Listo.")
