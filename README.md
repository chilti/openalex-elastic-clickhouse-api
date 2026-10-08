# OpenAlex ClickHouse API

> **Fork of [ourresearch/openalex-elastic-api](https://github.com/ourresearch/openalex-elastic-api)**
> Licensed under the [MIT License](LICENSE) — © 2021 OurResearch

This fork replaces the Elasticsearch backend with **ClickHouse** to enable self-hosted, high-performance querying of the full [OpenAlex](https://openalex.org/) dataset (~260M works, ~337M authors, ~110K institutions, ~250K sources).

## What's Different from Upstream

### New: `clickhouse_api/` Module

A complete ClickHouse backend that plugs into the existing Flask API without modifying the original endpoint structure.

| File | Purpose |
|---|---|
| `clickhouse.py` | Query builder with materialized column routing, token-based search, accent-insensitive regex matching, and automatic ID normalization |
| `optimize_v2.py` | Schema optimization: materializes frequently-queried fields from the `raw_data` JSON blob into dedicated columns |
| `load_openalex_clickhouse.py` | Bulk loader for ingesting OpenAlex snapshot data into ClickHouse |
| `openalex_schemas.sql` | Complete DDL schemas for all OpenAlex entity tables, skip indexes, and flat analytical layer |
| `init_openalex_schemas.py` | Automated schema initializer using ClickHouse Python driver |
| `GUIA_CARGA_SNAPSHOT_OPENALEX.md` | Comprehensive guide with official download links, AWS S3 commands, and bulk ingestion workflows |
| `.env` | Connection credentials (not committed) |

### Modified Upstream Files

| File | Change |
|---|---|
| `settings.py` | Added ClickHouse configuration variables and `USE_CLICKHOUSE` flag |
| `core/shared_view.py` | Routes queries to ClickHouse when `USE_CLICKHOUSE=true`, with automatic fallback to Elasticsearch |
| `docker-compose.yml` | Configured for self-hosted deployment with Redis on `localhost:5012` |

### Key Features

- **Token-Based Search** — Queries are split into tokens and matched independently, making search order-agnostic (`rafael torres cordoba` finds `Torres-Córdoba, Rafael`)
- **Accent-Insensitive Search** — Vowels are expanded into regex character classes (`o` → `[oóòôö]`) so searches ignore diacritics
- **Materialized Columns** — High-traffic fields (`title`, `display_name`, `doi`, `orcid`, `publication_year`, `source_id`, etc.) are extracted from JSON into native columns for sub-second queries
- **ID Normalization** — Short IDs are automatically expanded (`A5026648282` → `https://openalex.org/A5026648282`, `0000-0001-5448-8230` → `https://orcid.org/0000-0001-5448-8230`)
- **Filter Support** — `id`, `doi`, `orcid`, `institutions.ror`, `authorships.author.id`, `primary_topic.id`, date ranges, and pipe-separated OR values

## Quick Start

### Prerequisites

- Docker & Docker Compose
- A ClickHouse server with the OpenAlex data loaded (see [Snapshot Ingestion Guide](clickhouse_api/GUIA_CARGA_SNAPSHOT_OPENALEX.md))

### Configuration

Create `clickhouse_api/.env`:

```env
CH_HOST=your-clickhouse-host
CH_PORT=8123
CH_USER=your-user
CH_PASSWORD=your-password
CH_DATABASE=rag
```

### Run

```bash
docker compose up -d
```

The API will be available at `http://localhost:5012`.

### Example Queries

```bash
# Search authors by name (accent & order insensitive)
curl 'http://localhost:5012/authors?search=torres+cordoba+rafael'

# Filter by ORCID
curl 'http://localhost:5012/authors?filter=orcid:0000-0001-5448-8230'

# Filter by OpenAlex ID
curl 'http://localhost:5012/authors?filter=id:A5026648282'

# Search works by title
curl 'http://localhost:5012/works?search=machine+learning'

# Filter works by institution ROR
curl 'http://localhost:5012/works?filter=institutions.ror:03rzb4f20'

# Filter works by date range
curl 'http://localhost:5012/works?filter=from_publication_date:2023-01-01,to_publication_date:2024-12-31'
```

## OpenAlex Snapshot Ingestion & Schemas

Para descargar el snapshot oficial de OpenAlex (gratuito mediante AWS Open Data) y cargarlo a ClickHouse:

1. **Guía Completa:** Consulta [`clickhouse_api/GUIA_CARGA_SNAPSHOT_OPENALEX.md`](clickhouse_api/GUIA_CARGA_SNAPSHOT_OPENALEX.md).
2. **Crear Esquemas en ClickHouse:**
   ```bash
   /home/ambientesPy/revistaslatam/bin/python clickhouse_api/init_openalex_schemas.py
   ```
   *(o importa el DDL directamente desde [`clickhouse_api/openalex_schemas.sql`](clickhouse_api/openalex_schemas.sql))*
3. **Descargar Snapshot (JSON Lines):**
   ```bash
   aws s3 sync "s3://openalex/data/jsonl" "/mnt/expansion/openalex/openalex-snapshot/data" --no-sign-request
   ```
4. **Cargar a ClickHouse en Segundo Plano:**
   ```bash
   nohup /home/ambientesPy/revistaslatam/bin/python clickhouse_api/load_openalex_clickhouse.py \
     /mnt/expansion/openalex/openalex-snapshot/data \
     --workers 6 --batch-size 5000 --delay 1.0 \
     > clickhouse_api/carga_openalex.log 2>&1 &
   ```
5. **Monitorear Progreso:**
   ```bash
   /home/ambientesPy/revistaslatam/bin/python clickhouse_api/check_ingestion_status.py
   ```

## Architecture

```
Request → Flask (app.py)
        → Entity route (authors/, works/, etc.)
        → core/shared_view.py
        → IF USE_CLICKHOUSE:
            → clickhouse_api/clickhouse.py (ClickHouse)
          ELSE:
            → Elasticsearch (original behavior)
```

## Upstream

For the original project documentation, API reference, and bug reports unrelated to the ClickHouse backend, visit [github.com/ourresearch/openalex-elastic-api](https://github.com/ourresearch/openalex-elastic-api).

---

> **Note:** The ClickHouse backend, query optimizations, and search enhancements in this fork were developed with the assistance of **Antigravity** (Google DeepMind AI coding agent) and **Claude** (Anthropic AI), working in pair-programming sessions with [José Luis Jiménez Andrade](https://github.com/chilti).
