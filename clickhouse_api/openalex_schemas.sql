-- =============================================================================
-- ESQUEMAS DE CLICKHOUSE PARA OPENALEX (BASE DE DATOS: rag)
-- =============================================================================
-- Este archivo contiene las definiciones DDL de todas las tablas de OpenAlex:
-- 1. Control de archivos procesados (_processed_files)
-- 2. Entidades principales con columnas pre-calculadas e índices de salto
-- 3. Entidades taxonómicas y complementarias
-- 4. Capa analítica aplanada (works_flat y vista materializada works_flat_mv)
-- 5. Tablas agregadas de métricas (SummingMergeTree)
-- =============================================================================

CREATE DATABASE IF NOT EXISTS rag;

-- -----------------------------------------------------------------------------
-- 1. TABLA DE CONTROL DE INGESTA
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS rag._processed_files
(
    `entity` String,
    `file_name` String,
    `processed_at` DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (entity, file_name)
SETTINGS index_granularity = 8192;

-- -----------------------------------------------------------------------------
-- 2. ENTIDADES PRINCIPALES (CON COLUMNAS OPTIMIZADAS E ÍNDICES)
-- -----------------------------------------------------------------------------

-- WORKS (Trabajos científicos / Publicaciones)
CREATE TABLE IF NOT EXISTS rag.works
(
    `id` String,
    `raw_data` String,
    `doi` String DEFAULT JSONExtractString(raw_data, 'doi'),
    `title` String DEFAULT JSONExtractString(raw_data, 'title'),
    `publication_year` Int32 DEFAULT JSONExtractInt(raw_data, 'publication_year'),
    `cited_by_count` Int64 DEFAULT JSONExtractInt(raw_data, 'cited_by_count'),
    `is_oa` String DEFAULT JSONExtractString(raw_data, 'open_access', 'is_oa'),
    `is_xpac` String DEFAULT JSONExtractString(raw_data, 'is_xpac'),
    `type` String DEFAULT JSONExtractString(raw_data, 'type'),
    `source_id` String DEFAULT JSONExtractString(raw_data, 'primary_location', 'source', 'id'),
    `primary_topic_id` String DEFAULT JSONExtractString(raw_data, 'primary_topic', 'id'),
    `institution_ids` Array(String) DEFAULT arrayDistinct(arrayFlatten(arrayMap(x -> arrayMap(i -> JSONExtractString(i, 'id'), JSONExtractArrayRaw(x, 'institutions')), JSONExtractArrayRaw(raw_data, 'authorships')))),
    `author_names` Array(String) DEFAULT arrayDistinct(arrayFlatten(arrayMap(x -> [x.1.1, x.2], JSONExtract(raw_data, 'authorships', 'Array(Tuple(author Tuple(display_name String), raw_author_name String))')))),
    `institution_rors` Array(String) DEFAULT arrayDistinct(arrayFlatten(arrayMap(x -> arrayMap(i -> JSONExtractString(i, 'ror'), JSONExtractArrayRaw(x, 'institutions')), JSONExtractArrayRaw(raw_data, 'authorships')))),
    `institution_names` Array(String) DEFAULT arrayDistinct(arrayFlatten(arrayMap(x -> arrayMap(i -> JSONExtractString(i, 'display_name'), JSONExtractArrayRaw(x, 'institutions')), JSONExtractArrayRaw(raw_data, 'authorships')))),
    `subfield` String DEFAULT JSONExtractString(raw_data, 'primary_topic', 'subfield', 'display_name'),
    `field` String DEFAULT JSONExtractString(raw_data, 'primary_topic', 'field', 'display_name'),
    `domain` String DEFAULT JSONExtractString(raw_data, 'primary_topic', 'domain', 'display_name'),
    `topic` String DEFAULT JSONExtractString(raw_data, 'primary_topic', 'display_name'),
    `language` String DEFAULT JSONExtractString(raw_data, 'language'),
    `oa_status` String DEFAULT JSONExtractString(raw_data, 'open_access', 'oa_status'),
    `fwci` Float32 DEFAULT toFloat32OrZero(JSONExtractString(raw_data, 'fwci')),
    `percentile` Float32 DEFAULT toFloat32OrZero(JSONExtractString(raw_data, 'citation_normalized_percentile', 'value')),
    `is_top_10` UInt8 DEFAULT coalesce(JSONExtractBool(raw_data, 'citation_normalized_percentile', 'is_in_top_10_percent'), JSONExtractBool(raw_data, 'is_in_top_10_percent'), 0),
    `is_top_1` UInt8 DEFAULT coalesce(JSONExtractBool(raw_data, 'citation_normalized_percentile', 'is_in_top_1_percent'), JSONExtractBool(raw_data, 'is_in_top_1_percent'), 0),
    `country_code` String DEFAULT JSONExtractString(raw_data, 'authorships', 1, 'institutions', 1, 'country_code'),
    `all_country_codes` Array(String) DEFAULT arrayDistinct(arrayFlatten(arrayMap(x -> arrayMap(i -> i.1, x.1), JSONExtract(raw_data, 'authorships', 'Array(Tuple(institutions Array(Tuple(String))))')))),
    `source_type` String DEFAULT JSONExtractString(raw_data, 'primary_location', 'source', 'type'),
    `sdg_ids` Array(String) DEFAULT arrayMap(x -> x.1, arrayFilter(x -> (x.3) >= 0.4, JSONExtract(raw_data, 'sustainable_development_goals', 'Array(Tuple(String, String, Float32))'))),
    `awards` Array(String) DEFAULT arrayMap(x -> x.3, JSONExtract(raw_data, 'grants', 'Array(Tuple(String, String, String))')),
    `concept_ids` Array(String) DEFAULT arrayMap(x -> x.1, JSONExtract(raw_data, 'concepts', 'Array(Tuple(String, String, String, Int8, Float32))')),
    `updated_date` String DEFAULT JSONExtractString(raw_data, 'updated_date'),
    INDEX doi_idx doi TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX source_id_idx source_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX author_names_idx author_names TYPE tokenbf_v1(512, 3, 0) GRANULARITY 1,
    INDEX inst_rors_idx institution_rors TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX inst_names_idx institution_names TYPE tokenbf_v1(512, 3, 0) GRANULARITY 1,
    INDEX subfield_idx subfield TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX sdg_idx sdg_ids TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX concept_idx concept_ids TYPE tokenbf_v1(512, 3, 0) GRANULARITY 1,
    INDEX country_idx all_country_codes TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = ReplacingMergeTree
ORDER BY id
SETTINGS index_granularity = 8192;

-- AUTHORS (Autores e investigadores)
CREATE TABLE IF NOT EXISTS rag.authors
(
    `id` String,
    `raw_data` String,
    `display_name` String DEFAULT JSONExtractString(raw_data, 'display_name'),
    `orcid` String DEFAULT JSONExtractString(raw_data, 'orcid'),
    `works_count` Int64 DEFAULT JSONExtractInt(raw_data, 'works_count'),
    `cited_by_count` Int64 DEFAULT JSONExtractInt(raw_data, 'cited_by_count'),
    `updated_date` String DEFAULT JSONExtractString(raw_data, 'updated_date'),
    `last_known_institution_name` String MATERIALIZED JSONExtractString(raw_data, 'last_known_institution', 'display_name'),
    `ids` String MATERIALIZED JSONExtractString(raw_data, 'ids'),
    INDEX auth_name_idx display_name TYPE ngrambf_v1(4, 1024, 2, 1) GRANULARITY 1,
    INDEX orcid_idx orcid TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192;

-- INSTITUTIONS (Instituciones de afiliación)
CREATE TABLE IF NOT EXISTS rag.institutions
(
    `id` String,
    `raw_data` String,
    `ror` String DEFAULT JSONExtractString(raw_data, 'ror'),
    `display_name` String DEFAULT JSONExtractString(raw_data, 'display_name'),
    `country_code` String DEFAULT JSONExtractString(raw_data, 'country_code'),
    `type` String DEFAULT JSONExtractString(raw_data, 'type'),
    `works_count` Int32 DEFAULT JSONExtractInt(raw_data, 'works_count'),
    `cited_by_count` Int64 DEFAULT JSONExtractInt(raw_data, 'cited_by_count'),
    `updated_date` String DEFAULT JSONExtractString(raw_data, 'updated_date'),
    INDEX ror_idx ror TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX inst_name_idx display_name TYPE ngrambf_v1(4, 1024, 2, 1) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192;

-- SOURCES (Revistas, repositorios, conferencias)
CREATE TABLE IF NOT EXISTS rag.sources
(
    `id` String,
    `raw_data` String,
    `display_name` String DEFAULT JSONExtractString(raw_data, 'display_name'),
    `issn_l` String DEFAULT JSONExtractString(raw_data, 'issn_l'),
    `type` String DEFAULT JSONExtractString(raw_data, 'type'),
    `country_code` String DEFAULT JSONExtractString(raw_data, 'country_code'),
    `works_count` Int32 DEFAULT JSONExtractInt(raw_data, 'works_count'),
    `cited_by_count` Int64 DEFAULT JSONExtractInt(raw_data, 'cited_by_count'),
    `updated_date` String DEFAULT JSONExtractString(raw_data, 'updated_date'),
    INDEX issn_l_idx issn_l TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX source_name_idx display_name TYPE ngrambf_v1(4, 1024, 2, 1) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192;

-- TOPICS (Tópicos temáticos con jerarquía)
CREATE TABLE IF NOT EXISTS rag.topics
(
    `id` String,
    `raw_data` String,
    `display_name` String DEFAULT JSONExtractString(raw_data, 'display_name'),
    `subfield` String DEFAULT JSONExtractString(raw_data, 'subfield', 'display_name'),
    `field` String DEFAULT JSONExtractString(raw_data, 'field', 'display_name'),
    `domain` String DEFAULT JSONExtractString(raw_data, 'domain', 'display_name'),
    `works_count` Int64 DEFAULT JSONExtractInt(raw_data, 'works_count'),
    `cited_by_count` Int64 DEFAULT JSONExtractInt(raw_data, 'cited_by_count')
)
ENGINE = ReplacingMergeTree
ORDER BY id
SETTINGS index_granularity = 8192;

-- CONCEPTS (Conceptos temáticos / jerárquicos)
CREATE TABLE IF NOT EXISTS rag.concepts
(
    `id` String,
    `raw_data` String,
    `display_name` String DEFAULT JSONExtractString(raw_data, 'display_name'),
    `level` Int32 DEFAULT JSONExtractInt(raw_data, 'level'),
    `works_count` Int64 DEFAULT JSONExtractInt(raw_data, 'works_count'),
    `cited_by_count` Int64 DEFAULT JSONExtractInt(raw_data, 'cited_by_count')
)
ENGINE = ReplacingMergeTree
ORDER BY id
SETTINGS index_granularity = 8192;

-- PUBLISHERS (Editoriales)
CREATE TABLE IF NOT EXISTS rag.publishers
(
    `id` String,
    `raw_data` String,
    `display_name` String DEFAULT JSONExtractString(raw_data, 'display_name'),
    `country_codes` Array(String) DEFAULT JSONExtract(raw_data, 'country_codes', 'Array(String)'),
    `works_count` Int64 DEFAULT JSONExtractInt(raw_data, 'works_count'),
    `cited_by_count` Int64 DEFAULT JSONExtractInt(raw_data, 'cited_by_count')
)
ENGINE = ReplacingMergeTree
ORDER BY id
SETTINGS index_granularity = 8192;

-- FUNDERS (Financiadores)
CREATE TABLE IF NOT EXISTS rag.funders
(
    `id` String,
    `raw_data` String,
    `display_name` String DEFAULT JSONExtractString(raw_data, 'display_name'),
    `ror` String DEFAULT JSONExtractString(raw_data, 'ror')
)
ENGINE = ReplacingMergeTree
ORDER BY id
SETTINGS index_granularity = 8192;

-- -----------------------------------------------------------------------------
-- 3. ENTIDADES TAXONÓMICAS Y AUXILIARES
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS rag.awards (
    `id` String,
    `raw_data` String
) ENGINE = ReplacingMergeTree ORDER BY id SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS rag.continents (
    `id` String,
    `raw_data` String
) ENGINE = ReplacingMergeTree ORDER BY id SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS rag.countries (
    `id` String,
    `raw_data` String
) ENGINE = ReplacingMergeTree ORDER BY id SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS rag.domains (
    `id` String,
    `raw_data` String
) ENGINE = ReplacingMergeTree ORDER BY id SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS rag.fields (
    `id` String,
    `raw_data` String
) ENGINE = ReplacingMergeTree ORDER BY id SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS rag.`institution-types` (
    `id` String,
    `raw_data` String
) ENGINE = ReplacingMergeTree ORDER BY id SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS rag.keywords (
    `id` String,
    `raw_data` String
) ENGINE = ReplacingMergeTree ORDER BY id SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS rag.languages (
    `id` String,
    `raw_data` String
) ENGINE = ReplacingMergeTree ORDER BY id SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS rag.licenses (
    `id` String,
    `raw_data` String
) ENGINE = ReplacingMergeTree ORDER BY id SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS rag.sdgs (
    `id` String,
    `raw_data` String
) ENGINE = ReplacingMergeTree ORDER BY id SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS rag.`source-types` (
    `id` String,
    `raw_data` String
) ENGINE = ReplacingMergeTree ORDER BY id SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS rag.subfields (
    `id` String,
    `raw_data` String
) ENGINE = ReplacingMergeTree ORDER BY id SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS rag.`work-types` (
    `id` String,
    `raw_data` String
) ENGINE = ReplacingMergeTree ORDER BY id SETTINGS index_granularity = 8192;

-- -----------------------------------------------------------------------------
-- 4. CAPA ANALÍTICA APLANADA (WORKS_FLAT) Y VISTA MATERIALIZADA
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS rag.works_flat
(
    `id`                       String,
    `doi`                      String,
    `title`                    String,
    `abstract`                 String,
    `publication_year`         UInt16,
    `publication_date`         Date,
    `type`                     LowCardinality(String),
    `language`                 LowCardinality(String),
    `cited_by_count`           UInt32,
    `fwci`                     Float32,
    `percentile`               Float32,
    `is_top_10`                UInt8,
    `is_top_1`                 UInt8,
    `referenced_works_count`   UInt32,
    `source_id`                LowCardinality(String),
    `source_type`              LowCardinality(String),
    `is_oa`                    UInt8,
    `oa_status`                LowCardinality(String),
    `topic_id`                 LowCardinality(String),
    `subfield_id`              LowCardinality(String),
    `subfield_name`            LowCardinality(String),
    `field_name`               LowCardinality(String),
    `domain_name`              LowCardinality(String),
    `author_ids`               Array(String),
    `institution_ids`          Array(String),
    `institution_types`        Array(LowCardinality(String)),
    `country_codes`            Array(LowCardinality(String)),
    `referenced_works`         Array(String),
    `concepts`                 Array(LowCardinality(String)),
    `pmid`                     String,
    `mag_id`                   String,
    `is_retracted`             UInt8,
    `is_paratext`              UInt8,
    `volume`                   String,
    `issue`                    String,
    `first_page`               String,
    `last_page`                String,
    `all_topics`               Array(LowCardinality(String)),
    `keywords`                 Array(String),
    `mesh`                     Array(String),
    `funder_ids`               Array(String),
    `funder_names`             Array(String),
    `sdgs`                     Array(LowCardinality(String)),
    `raw_data`                 String,
    `updated_date`             String,
    `is_xpac`                  String,
    `author_names`             Array(String),
    `institution_rors`         Array(String),
    `institution_names`        Array(String),
    `primary_topic_id`         String,
    `subfield`                 String,
    `field`                    String,
    `domain`                   String,
    `topic`                    String,
    `country_code`             String,
    `sdg_ids`                  Array(String),
    `awards`                   Array(String),
    `concept_ids`              Array(String),
    `all_country_codes`        Array(String),
    `apc_paid_usd`             Float64,
    `apc_list_usd`             Float64,
    `counts_by_year`           String,
    `is_doaj_indexed`          UInt8,
    `is_doaj_journal`          UInt8,
    `is_core_journal`          UInt8,
    `has_repository_fulltext`  UInt8,
    `license`                  String,
    `journal_is_in_doaj`       UInt8,
    `journal_is_core`          UInt8,
    `any_repository_has_fulltext` UInt8
)
ENGINE = ReplacingMergeTree
PARTITION BY publication_year
ORDER BY id
SETTINGS index_granularity = 8192;

-- -----------------------------------------------------------------------------
-- 5. TABLAS AGREGADAS DE MÉTRICAS (SummingMergeTree)
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS rag.summing_subfield_metrics
(
    `subfield` String,
    `year` UInt16,
    `country_code` String,
    `source_id` String,
    `topic` String,
    `doc_count` UInt64,
    `fwci_sum` Float64,
    `percentile_sum` Float64,
    `top_10_sum` UInt64,
    `top_1_sum` UInt64,
    `gold_count` UInt64,
    `diamond_count` UInt64,
    `green_count` UInt64,
    `hybrid_count` UInt64,
    `bronze_count` UInt64,
    `closed_count` UInt64,
    `lang_en` UInt64,
    `lang_es` UInt64,
    `lang_pt` UInt64
)
ENGINE = SummingMergeTree
ORDER BY (subfield, year, country_code, source_id, topic)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS rag.summing_subfield_inst_metrics
(
    `subfield` String,
    `year` UInt16,
    `institution_id` String,
    `topic` String,
    `source_id` String,
    `doc_count` UInt64,
    `fwci_sum` Float64,
    `percentile_sum` Float64,
    `top_1_sum` UInt64,
    `top_10_sum` UInt64,
    `top_25_sum` UInt64,
    `citations_sum` UInt64,
    `intl_collab_count` UInt64,
    `sdg_count` UInt64,
    `award_count` UInt64,
    `review_count` UInt64,
    `gold_count` UInt64,
    `diamond_count` UInt64,
    `green_count` UInt64,
    `hybrid_count` UInt64,
    `bronze_count` UInt64,
    `closed_count` UInt64,
    `lang_en` UInt64,
    `lang_es` UInt64,
    `lang_pt` UInt64
)
ENGINE = SummingMergeTree
ORDER BY (subfield, year, institution_id, topic, source_id)
SETTINGS index_granularity = 8192;
