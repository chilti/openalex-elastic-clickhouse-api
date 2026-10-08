# Guía Oficial: Descarga del Snapshot de OpenAlex y Carga Masiva a ClickHouse

Esta guía detalla el procedimiento completo y probado para descargar el snapshot oficial de [OpenAlex](https://openalex.org) desde Amazon S3 (gratuito vía AWS Open Data) y cargarlo de forma controlada a ClickHouse utilizando el script `load_openalex_clickhouse.py`.

---

## 1. Enlaces y Recursos Oficiales

OpenAlex distribuye su base de datos completa como un snapshot público alojado en Amazon S3. Gracias al programa **AWS Open Data Sponsorship**, la descarga es completamente gratuita y no requiere cuenta de AWS ni credenciales.

- **Documentación oficial del Snapshot:** [https://docs.openalex.org/download-all-data/openalex-snapshot](https://docs.openalex.org/download-all-data/openalex-snapshot)
- **Tutorial oficial de descarga:** [https://docs.openalex.org/tutorials/download-the-snapshot/](https://docs.openalex.org/tutorials/download-the-snapshot/)
- **Explorador interactivo del bucket S3 en navegador:** [https://openalex.s3.amazonaws.com/browse.html](https://openalex.s3.amazonaws.com/browse.html)
- **Registro en AWS Open Data:** [https://registry.opendata.aws/openalex/](https://registry.opendata.aws/openalex/)
- **Notas de lanzamiento y versiones:** [https://github.com/ourresearch/openalex-guts/blob/main/files-for-datadumps/standard-format/RELEASE_NOTES.txt](https://github.com/ourresearch/openalex-guts/blob/main/files-for-datadumps/standard-format/RELEASE_NOTES.txt)

### Formatos disponibles en el bucket S3

El bucket `s3://openalex/data/` se distribuye en dos formatos paralelos:
1. **JSON Lines comprimido (`data/jsonl/`):** Archivos `part_*.gz` con un objeto JSON por línea. **Este es el formato requerido por `load_openalex_clickhouse.py`**.
2. **Apache Parquet (`data/parquet/`):** Archivos `.parquet` para motores como DuckDB o Spark.

> [!IMPORTANT]
> Descarga **únicamente** la carpeta `data/jsonl/` para evitar duplicar la descarga. El snapshot en JSON Lines comprimido ronda entre 600 GB y 750 GB (descomprimido supera los 3 TB). Asegúrate de contar con al menos 1 TB libre en el disco de destino (por ejemplo, en `/mnt/expansion/`).

---

## 2. Descarga del Snapshot desde AWS S3

### Requisitos previos
- Tener instalado el cliente de AWS CLI (`aws --version`).
- No es necesario configurar credenciales; se utiliza la bandera `--no-sign-request`.

### 2.1 Descargar todo el snapshot en JSON Lines
Ejecuta el comando `aws s3 sync` apuntando a la carpeta de destino:

```bash
mkdir -p /mnt/expansion/openalex/openalex-snapshot/data

aws s3 sync "s3://openalex/data/jsonl" "/mnt/expansion/openalex/openalex-snapshot/data" \
  --no-sign-request
```

### 2.2 Descargas selectivas por entidad (Recomendado si hay espacio limitado)
Si solo requieres ciertas entidades (por ejemplo, `works`, `authors`, `institutions` o `sources`), descarga únicamente sus subdirectorios:

```bash
DEST_BASE="/mnt/expansion/openalex/openalex-snapshot/data"

# Works (la entidad más pesada, ~500-600 GiB)
aws s3 sync "s3://openalex/data/jsonl/works" "$DEST_BASE/works" --no-sign-request

# Authors (~65 GiB)
aws s3 sync "s3://openalex/data/jsonl/authors" "$DEST_BASE/authors" --no-sign-request

# Institutions (~200 MiB)
aws s3 sync "s3://openalex/data/jsonl/institutions" "$DEST_BASE/institutions" --no-sign-request

# Sources (~350 MiB)
aws s3 sync "s3://openalex/data/jsonl/sources" "$DEST_BASE/sources" --no-sign-request

# Topics, subfields, fields, domains, etc.
aws s3 sync "s3://openalex/data/jsonl/topics" "$DEST_BASE/topics" --no-sign-request
aws s3 sync "s3://openalex/data/jsonl/subfields" "$DEST_BASE/subfields" --no-sign-request
aws s3 sync "s3://openalex/data/jsonl/fields" "$DEST_BASE/fields" --no-sign-request
aws s3 sync "s3://openalex/data/jsonl/domains" "$DEST_BASE/domains" --no-sign-request
aws s3 sync "s3://openalex/data/jsonl/publishers" "$DEST_BASE/publishers" --no-sign-request
aws s3 sync "s3://openalex/data/jsonl/funders" "$DEST_BASE/funders" --no-sign-request
```

### 2.3 Sincronización incremental y eliminación de particiones obsoletas
Cuando se actualiza un snapshot existente, OpenAlex reubica registros en particiones `updated_date=YYYY-MM-DD`. Agrega la bandera `--delete` para eliminar en disco los archivos antiguos que ya no existan en el origen S3:

```bash
aws s3 sync "s3://openalex/data/jsonl" "/mnt/expansion/openalex/openalex-snapshot/data" \
  --no-sign-request \
  --delete
```

> [!WARNING]
> Si sincronizaste con `--delete`, debes ejecutar `load_openalex_clickhouse.py` con la bandera `--reconcile` para que ClickHouse limpie y recargue de forma consistente las entidades afectadas.

---

## 3. Estructura del Snapshot Descargado

Al finalizar la descarga, el directorio de datos debe tener esta jerarquía:

```
/mnt/expansion/openalex/openalex-snapshot/data/
├── works/
│   ├── manifest.json
│   ├── deleted_ids.csv.gz
│   └── updated_date=YYYY-MM-DD/
│       ├── part_0000.gz
│       └── part_0001.gz
├── authors/
│   ├── manifest.json
│   └── updated_date=YYYY-MM-DD/
│       └── part_*.gz
├── institutions/
├── sources/
├── topics/
└── ... (concepts, publishers, funders, etc.)
```

---

## 4. Creación e Inicialización de Esquemas en ClickHouse

Antes de la carga (o para preparar una instalación limpia), se deben crear las tablas correspondientes en ClickHouse.

Se han preparado dos herramientas en `clickhouse_api/`:
1. **`openalex_schemas.sql`**: Definición DDL completa de todas las tablas (control de ingesta, 21 entidades crudas, columnas pre-extraídas, índices Bloom Filter y capa plana analítica).
2. **`init_openalex_schemas.py`**: Script en Python que aplica las sentencias de forma automática conectándose a la base de datos configurada en `.env`.

### 4.1 Configuración de credenciales (`clickhouse_api/.env`)
Verifica que el archivo `/mnt/expansion/desplegados/openalex-elastic-clickhouse-api/clickhouse_api/.env` contenga la configuración correcta:

```env
USE_CLICKHOUSE=true
CH_HOST=10.90.0.87
CH_PORT=8124
CH_USER=rag_user
CH_PASSWORD=tu_contraseña
CH_DATABASE=rag
```

### 4.2 Ejecutar la creación de esquemas
Utiliza el ambiente de Python designado (`/home/ambientesPy/revistaslatam`):

```bash
/home/ambientesPy/revistaslatam/bin/python /mnt/expansion/desplegados/openalex-elastic-clickhouse-api/clickhouse_api/init_openalex_schemas.py
```

O si prefieres ejecutar directamente el cliente de ClickHouse por CLI:

```bash
clickhouse-client --host 10.90.0.87 --port 9000 --user rag_user --password "..." --multiquery < /mnt/expansion/desplegados/openalex-elastic-clickhouse-api/clickhouse_api/openalex_schemas.sql
```

---

## 5. Carga de Datos a ClickHouse con `load_openalex_clickhouse.py`

El script `clickhouse_api/load_openalex_clickhouse.py` está diseñado para ingestas masivas de alta resiliencia. Sus principales características son:
- **Descubrimiento automático de entidades** escaneando las carpetas con archivos `.gz`.
- **Procesamiento paralelo controlado** con `ProcessPoolExecutor` por lotes (chunks) para no saturar los merges de ClickHouse.
- **Idempotencia y reanudación automática:** Registra los archivos completados en la tabla `_processed_files`. Si el proceso se detiene, continuará únicamente con los archivos pendientes.
- **Reintentos automáticos con backoff exponencial** en caso de timeout o microcortes de red.

### 5.1 Parámetros del Script

| Parámetro | Default | Descripción |
|---|---|---|
| `snapshot_dir` | *(Requerido)* | Ruta al directorio raíz que contiene las carpetas de entidades (`data`). |
| `--workers` | `4` | Número de procesos paralelos para leer `.gz` e insertar. |
| `--batch-size` | `5000` | Filas por cada comando `client.insert()`. |
| `--delay` | `1.0` | Segundos de pausa entre inserciones y entre chunks para permitir a ClickHouse fusionar partes. |
| `--entity-pause` | `5.0` | Segundos de pausa al completar una entidad antes de pasar a la siguiente. |
| `--reconcile` | `False` | Detecta si hay archivos en `_processed_files` que fueron borrados del disco (`sync --delete`). Si los detecta, hace `TRUNCATE` de la entidad y la recarga limpia. |
| `--entities` | *(Todas)* | Lista de entidades específicas a cargar (ej. `--entities works authors`). |
| `--env-file` | `clickhouse_api/.env` | Ruta alternativa al archivo de variables de entorno. |

### 5.2 Comandos de Ejecución

#### Opción A: Carga estándar interactiva
```bash
/home/ambientesPy/revistaslatam/bin/python /mnt/expansion/desplegados/openalex-elastic-clickhouse-api/clickhouse_api/load_openalex_clickhouse.py \
  /mnt/expansion/openalex/openalex-snapshot/data
```

#### Opción B: Carga en segundo plano con `nohup` (Recomendada para producción)
Para procesos de larga duración, ejecútalo desatendido redirigiendo la salida a un log:

```bash
nohup /home/ambientesPy/revistaslatam/bin/python /mnt/expansion/desplegados/openalex-elastic-clickhouse-api/clickhouse_api/load_openalex_clickhouse.py \
  /mnt/expansion/openalex/openalex-snapshot/data \
  --workers 6 \
  --batch-size 5000 \
  --delay 1.0 \
  > /mnt/expansion/desplegados/openalex-elastic-clickhouse-api/clickhouse_api/carga_openalex.log 2>&1 &
```

Para seguir el avance en tiempo real:
```bash
tail -f /mnt/expansion/desplegados/openalex-elastic-clickhouse-api/clickhouse_api/carga_openalex.log
```

#### Opción C: Carga con Reconciliación tras `sync --delete`
```bash
/home/ambientesPy/revistaslatam/bin/python /mnt/expansion/desplegados/openalex-elastic-clickhouse-api/clickhouse_api/load_openalex_clickhouse.py \
  /mnt/expansion/openalex/openalex-snapshot/data \
  --reconcile
```

#### Opción D: Cargar solo entidades específicas
```bash
/home/ambientesPy/revistaslatam/bin/python /mnt/expansion/desplegados/openalex-elastic-clickhouse-api/clickhouse_api/load_openalex_clickhouse.py \
  /mnt/expansion/openalex/openalex-snapshot/data \
  --entities institutions sources topics publishers funders
```

---

## 6. Monitoreo y Verificación del Progreso

### 6.1 Script de estado
Usa el script de diagnóstico incluido:

```bash
/home/ambientesPy/revistaslatam/bin/python /mnt/expansion/desplegados/openalex-elastic-clickhouse-api/clickhouse_api/check_ingestion_status.py
```

### 6.2 Consultas directas en ClickHouse
Puedes consultar el avance de archivos procesados y el volumen de filas:

```sql
-- Archivos procesados por entidad
SELECT entity, count() AS total_archivos, min(processed_at), max(processed_at)
FROM rag._processed_files
GROUP BY entity
ORDER BY total_archivos DESC;

-- Filas actuales por tabla
SELECT
    table,
    formatReadableQuantity(sum(rows)) AS filas,
    formatReadableSize(sum(bytes)) AS tamano_disco
FROM system.parts
WHERE database = 'rag' AND active = 1
GROUP BY table
ORDER BY sum(rows) DESC;
```

---

## 7. Optimización y Post-procesamiento

Una vez cargados los datos crudos en formato JSON, se pueden materializar columnas de alto tráfico y generar la tabla aplanada analítica:

1. **Materializar columnas e índices de salto en tablas principales:**
   ```bash
   /home/ambientesPy/revistaslatam/bin/python /mnt/expansion/desplegados/openalex-elastic-clickhouse-api/clickhouse_api/optimize_v2.py
   ```
2. **Poblar la capa analítica aplanada (`works_flat`):**
   ```bash
   /home/ambientesPy/revistaslatam/bin/python /mnt/expansion/desplegados/openalex-elastic-clickhouse-api/scripts/migrate_to_flat_table.py
   ```
3. **Poblar las métricas pre-agregadas (`summing_subfield_metrics`):**
   ```bash
   /home/ambientesPy/revistaslatam/bin/python /mnt/expansion/desplegados/openalex-elastic-clickhouse-api/scripts/create_subfield_mv.py
   ```
