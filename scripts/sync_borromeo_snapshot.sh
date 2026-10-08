#!/usr/bin/env bash
set -eo pipefail

LOG_FILE="/mnt/expansion/openalex/rsync_borromeo.log"
SRC="vega@borromeo.c3.unam.mx:/storage/carrillo_g/jimenez/openalex-snapshot/"
DEST="/mnt/expansion/openalex/openalex-snapshot/"
KEY="/home/jlja/.ssh/id_rsa_borromeo"

mkdir -p "$DEST"
echo "========================================================" >> "$LOG_FILE"
echo "🚀 Iniciando descarga de OpenAlex Snapshot desde Borromeo" >> "$LOG_FILE"
echo "Fecha y hora de inicio: $(date)" >> "$LOG_FILE"
echo "Origen:      $SRC" >> "$LOG_FILE"
echo "Destino:     $DEST" >> "$LOG_FILE"
echo "========================================================" >> "$LOG_FILE"

rsync -rtv \
  --no-perms --no-owner --no-group \
  --inplace \
  --partial \
  --info=progress2,stats2 \
  -e "ssh -i $KEY -o ServerAliveInterval=30 -o ServerAliveCountMax=10 -o BatchMode=yes" \
  "$SRC" "$DEST" >> "$LOG_FILE" 2>&1

EXIT_CODE=$?
echo "========================================================" >> "$LOG_FILE"
echo "🏁 Proceso finalizado con código: $EXIT_CODE" >> "$LOG_FILE"
echo "Fecha y hora de fin:    $(date)" >> "$LOG_FILE"
echo "========================================================" >> "$LOG_FILE"
exit $EXIT_CODE
