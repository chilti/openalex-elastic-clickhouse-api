"""
explore_snapshot.py
-------------------
Explora el snapshot de OpenAlex en el servidor remoto vía SSH y lo compara
contra _processed_files en ClickHouse para saber qué entidades
necesitan recargarse.

Uso:
    python clickhouse_api/explore_snapshot.py

Salida:
    - Reporte en consola
    - Archivo: snapshot_diagnosis.json  (para uso de otros scripts)
"""

import os
import json
import paramiko
import clickhouse_connect
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime

# ─────────────────────────────────────────────
# Configuración SSH (remoto)
# ─────────────────────────────────────────────
load_dotenv()
SSH_HOST     = os.environ.get("SSH_HOST", "")
SSH_USER     = os.environ.get("SSH_USER", "")
SSH_PASSWORD = os.environ.get("SSH_PASSWORD", "")
SNAPSHOT_DIR = os.environ.get("SNAPSHOT_DIR", "")

# ─────────────────────────────────────────────
# Configuración ClickHouse (local)
# ─────────────────────────────────────────────
CH_HOST     = os.environ.get("CH_HOST", "10.90.0.87")
CH_PORT     = int(os.environ.get("CH_PORT", 8124))
CH_USER     = os.environ.get("CH_USER", "rag_user")
CH_PASSWORD = os.environ.get("CH_PASSWORD", "")
CH_DATABASE = os.environ.get("CH_DATABASE", "rag")

# ─────────────────────────────────────────────────────────────────────────────

def get_ssh_client():
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(SSH_HOST, username=SSH_USER, password=SSH_PASSWORD, timeout=15)
    return client


def ssh_run(client, cmd):
    """Ejecuta un comando en el servidor remoto y devuelve stdout como string."""
    _, stdout, stderr = client.exec_command(cmd, timeout=120)
    out = stdout.read().decode("utf-8", errors="replace").strip()
    err = stderr.read().decode("utf-8", errors="replace").strip()
    if err:
        # Mensajes informativos de du/find no son errores reales
        pass
    return out


def get_snapshot_entities(client):
    """
    Lista los subdirectorios de primer nivel del snapshot
    (cada uno es una entidad de OpenAlex).
    """
    out = ssh_run(client, f"ls -1 {SNAPSHOT_DIR}/")
    return [e.strip() for e in out.splitlines() if e.strip()]


def get_entity_stats(client, entity):
    """
    Para una entidad devuelve:
      - n_files    : cantidad de archivos .gz
      - total_size : tamaño total en bytes
      - sample_files: hasta 3 nombres de archivo de ejemplo
      - newest_file : fecha de modificación más reciente (ISO)
    """
    entity_path = f"{SNAPSHOT_DIR}/{entity}"

    # Conteo y tamaño total
    count_cmd = f"find {entity_path} -name '*.gz' | wc -l"
    size_cmd  = f"find {entity_path} -name '*.gz' -printf '%s\\n' | awk '{{s+=$1}} END{{print s+0}}'"
    # Muestra de nombres de archivo
    sample_cmd = f"find {entity_path} -name '*.gz' | head -3"
    # Fecha más reciente
    date_cmd = (
        f"find {entity_path} -name '*.gz' -printf '%T@\\n' "
        f"| sort -n | tail -1 | xargs -I{{}} date -d @{{}} '+%Y-%m-%d %H:%M:%S' 2>/dev/null || echo 'N/A'"
    )

    n_files    = int(ssh_run(client, count_cmd) or 0)
    total_size = int(ssh_run(client, size_cmd) or 0)
    samples    = [Path(f).name for f in ssh_run(client, sample_cmd).splitlines() if f.strip()]
    newest     = ssh_run(client, date_cmd) or "N/A"

    return {
        "n_files":    n_files,
        "total_size": total_size,
        "size_hr":    human_size(total_size),
        "sample_files": samples,
        "newest_file":  newest,
    }


def get_clickhouse_state():
    """
    Obtiene el estado de _processed_files en ClickHouse agrupado por entidad.
    Devuelve dict: {entity: {"n_processed": int, "last_processed": str}}
    """
    client = clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT,
        username=CH_USER, password=CH_PASSWORD,
        database=CH_DATABASE,
        secure=False, verify=False
    )
    res = client.query("""
        SELECT entity, count() as n, max(processed_at) as last_at
        FROM _processed_files
        GROUP BY entity
        ORDER BY entity
    """)
    state = {}
    for row in res.result_rows:
        state[row[0]] = {
            "n_processed": int(row[1]),
            "last_processed": str(row[2])
        }
    return state


def human_size(n_bytes):
    for unit in ["B", "KiB", "MiB", "GiB", "TiB"]:
        if n_bytes < 1024:
            return f"{n_bytes:.1f} {unit}"
        n_bytes /= 1024
    return f"{n_bytes:.1f} PiB"


def classify(snap_files, processed_files):
    """
    Determina el estado de carga de una entidad:
      OK           → mismo número de archivos procesados
      PARTIAL      → procesados < snapshot (faltan archivos nuevos)
      OVER         → procesados > snapshot (hay archivos "fantasma" borrados)
      EMPTY        → no hay ningún archivo procesado aún
    """
    if processed_files == 0:
        return "EMPTY"
    elif processed_files == snap_files:
        return "OK"
    elif processed_files < snap_files:
        return "PARTIAL"
    else:
        return "OVER (archivos borrados del snapshot pero aún en BD)"


# ─────────────────────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*70}")
    print(f"  DIAGNÓSTICO SNAPSHOT vs CLICKHOUSE — {datetime.now():%Y-%m-%d %H:%M}")
    print(f"{'='*70}")
    print(f"  SSH  : {SSH_USER}@{SSH_HOST}:{SNAPSHOT_DIR}")
    print(f"  CH   : {CH_HOST}:{CH_PORT}/{CH_DATABASE}")
    print(f"{'='*70}\n")

    # 1. Conectar SSH
    print(f"🔗 Conectando a {SSH_HOST}...")
    ssh = get_ssh_client()
    print("   ✅ Conexión SSH establecida\n")

    # 2. Obtener entidades del snapshot
    print("📂 Explorando snapshot...")
    entities = get_snapshot_entities(ssh)
    print(f"   Entidades encontradas: {entities}\n")

    # 3. Stats de cada entidad en el snapshot
    snapshot_stats = {}
    for entity in entities:
        print(f"   Analizando: {entity}...", end="", flush=True)
        snapshot_stats[entity] = get_entity_stats(ssh, entity)
        s = snapshot_stats[entity]
        print(f" {s['n_files']} archivos | {s['size_hr']}")

    ssh.close()

    # 4. Estado en ClickHouse
    print("\n🗄️  Consultando ClickHouse (_processed_files)...")
    ch_state = get_clickhouse_state()
    print(f"   Entidades con registros: {list(ch_state.keys())}\n")

    # 5. Reporte comparativo
    print(f"\n{'='*70}")
    print(f"  REPORTE COMPARATIVO")
    print(f"{'='*70}")
    fmt = "  {:<25} {:>8}  {:>8}  {:>12}  {}"
    print(fmt.format("ENTIDAD", "SNAPSHOT", "CH_PROC", "TAMAÑO", "ESTADO"))
    print(f"  {'-'*65}")

    diagnosis = {}
    needs_reload = []
    ok_entities  = []

    all_entities = sorted(set(list(entities) + list(ch_state.keys())))
    for entity in all_entities:
        snap  = snapshot_stats.get(entity, {})
        ch    = ch_state.get(entity, {})
        n_snap = snap.get("n_files", 0)
        n_ch   = ch.get("n_processed", 0)
        size   = snap.get("size_hr", "—")
        status = classify(n_snap, n_ch)

        icon = {"OK": "✅", "EMPTY": "🆕", "PARTIAL": "⚠️ ", "OVER (archivos borrados del snapshot pero aún en BD)": "❌"}.get(status, "?")
        short_status = status.split(" ")[0]

        print(fmt.format(entity, n_snap, n_ch, size, f"{icon}  {short_status}"))

        diagnosis[entity] = {
            "snapshot_files": n_snap,
            "ch_processed":   n_ch,
            "size":           snap.get("size_hr", "—"),
            "sample_files":   snap.get("sample_files", []),
            "newest_in_snap": snap.get("newest_file", "N/A"),
            "last_in_ch":     ch.get("last_processed", "N/A"),
            "status":         status,
        }

        if status != "OK":
            needs_reload.append(entity)
        else:
            ok_entities.append(entity)

    print(f"\n{'='*70}")
    print(f"  RESUMEN")
    print(f"{'='*70}")
    print(f"  ✅ OK (sin cambios):   {ok_entities}")
    print(f"  ⚠️  Necesitan recarga: {needs_reload}")

    # 6. Guardar JSON para uso de scripts de recarga
    output_path = Path(__file__).parent / "snapshot_diagnosis.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump({
            "generated_at": datetime.now().isoformat(),
            "ssh_host":     SSH_HOST,
            "snapshot_dir": SNAPSHOT_DIR,
            "entities":     diagnosis,
            "needs_reload": needs_reload,
            "ok_entities":  ok_entities,
        }, f, ensure_ascii=False, indent=2)

    print(f"\n  📄 Diagnóstico guardado en: {output_path}")
    print(f"\n{'='*70}\n")


if __name__ == "__main__":
    main()
