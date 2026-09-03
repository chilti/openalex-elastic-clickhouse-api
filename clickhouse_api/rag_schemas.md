# Esquemas de Base de Datos: rag

## Tabla: `_processed_files`
```sql
CREATE TABLE rag._processed_files
(
    `entity` String,
    `file_name` String,
    `processed_at` DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (entity, file_name)
SETTINGS index_granularity = 8192
```

## Tabla: `_tmp_test_join`
```sql
CREATE TABLE rag._tmp_test_join
(
    `id` String,
    `embedding_specter2` Array(Float32),
    `val_exists` UInt8
)
ENGINE = Join(ANY, LEFT, id)
```

## Tabla: `academics_all`
```sql
CREATE TABLE rag.academics_all
(
    `id` String,
    `name` String,
    `institution` String,
    `dependency` String,
    `subdependency` String,
    `snii_level` String,
    `orcid` String,
    `paper_count` UInt32,
    `citation_count` UInt32,
    `embedding_nomic` Array(Float32),
    `embedding_specter` Array(Float32),
    `embedding_fastrp` Array(Float32)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192
```

## Tabla: `authors`
```sql
CREATE TABLE rag.authors
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
SETTINGS index_granularity = 8192
```

## Tabla: `authors_seed_mexico`
```sql
CREATE TABLE rag.authors_seed_mexico
(
    `id` String,
    `display_name` String,
    `orcid` String,
    `ids` String,
    `raw_data` String
)
ENGINE = MergeTree
ORDER BY (display_name, id)
SETTINGS index_granularity = 8192
```

## Tabla: `awards`
```sql
CREATE TABLE rag.awards
(
    `id` String,
    `raw_data` String
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192
```

## Tabla: `concepts`
```sql
CREATE TABLE rag.concepts
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
SETTINGS index_granularity = 8192
```

## Tabla: `continents`
```sql
CREATE TABLE rag.continents
(
    `id` String,
    `raw_data` String
)
ENGINE = ReplacingMergeTree
ORDER BY id
SETTINGS index_granularity = 8192
```

## Tabla: `countries`
```sql
CREATE TABLE rag.countries
(
    `id` String,
    `raw_data` String
)
ENGINE = ReplacingMergeTree
ORDER BY id
SETTINGS index_granularity = 8192
```

## Tabla: `domains`
```sql
CREATE TABLE rag.domains
(
    `id` String,
    `raw_data` String
)
ENGINE = ReplacingMergeTree
ORDER BY id
SETTINGS index_granularity = 8192
```

## Tabla: `embeddings_cache`
```sql
CREATE TABLE rag.embeddings_cache
(
    `id` String COMMENT 'OpenAlex Work ID',
    `subfield_name` LowCardinality(String),
    `publication_year` UInt16,
    `embedding_specter2` Array(Float32) DEFAULT [],
    `embedding_scilbert` Array(Float32) DEFAULT [],
    `embedding_fastrp_cit` Array(Float32) DEFAULT [],
    `embedding_fastrp_het` Array(Float32) DEFAULT [],
    `embedding_umap_30d` Array(Float32) DEFAULT [],
    `specter2_at` Nullable(DateTime),
    `scilbert_at` Nullable(DateTime),
    `fastrp_cit_at` Nullable(DateTime),
    `fastrp_het_at` Nullable(DateTime),
    `umap_30d_at` Nullable(DateTime),
    `updated_at` DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (subfield_name, publication_year, id)
SETTINGS index_granularity = 8192
COMMENT 'Cache de embeddings ML por paper. Una columna por modelo. Nunca modifica works_flat.'
```

## Tabla: `fields`
```sql
CREATE TABLE rag.fields
(
    `id` String,
    `raw_data` String
)
ENGINE = ReplacingMergeTree
ORDER BY id
SETTINGS index_granularity = 8192
```

## Tabla: `funders`
```sql
CREATE TABLE rag.funders
(
    `id` String,
    `raw_data` String,
    `display_name` String DEFAULT JSONExtractString(raw_data, 'display_name'),
    `ror` String DEFAULT JSONExtractString(raw_data, 'ror')
)
ENGINE = ReplacingMergeTree
ORDER BY id
SETTINGS index_granularity = 8192
```

## Tabla: `institution-types`
*No se pudo obtener el esquema: Received ClickHouse exception, code: 62, server response: Code: 62. DB::Exception: Syntax error: failed at position 34 (-) (line 1, col 34): -types
 FORMAT Native. Expected one of: INTO OUTFILE, FORMAT, SETTINGS, ParallelWithClause, PARALLEL WITH, end of query. (SYNTAX_ERROR) (version 25.4.5.24 (official build)) (for url http://10.90.0.87:8124)*

## Tabla: `institutions`
```sql
CREATE TABLE rag.institutions
(
    `id` String,
    `raw_data` String,
    `display_name` String DEFAULT JSONExtractString(raw_data, 'display_name'),
    `ror` String DEFAULT JSONExtractString(raw_data, 'ror'),
    `type` String DEFAULT JSONExtractString(raw_data, 'type'),
    `works_count` Int64 DEFAULT JSONExtractInt(raw_data, 'works_count'),
    `cited_by_count` Int64 DEFAULT JSONExtractInt(raw_data, 'cited_by_count'),
    `updated_date` String DEFAULT JSONExtractString(raw_data, 'updated_date'),
    `country_code` String DEFAULT JSONExtractString(raw_data, 'country_code'),
    INDEX ror_idx ror TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX inst_name_idx display_name TYPE ngrambf_v1(4, 1024, 2, 1) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192
```

## Tabla: `institutions_seed_mexico`
```sql
CREATE TABLE rag.institutions_seed_mexico
(
    `id` String,
    `display_name` String,
    `ror` String,
    `type` String,
    `country_code` String,
    `city` String,
    `state` String,
    `acronyms` Array(String),
    `parents` Array(Tuple(
        id String,
        ror String,
        display_name String,
        country_code String,
        type String,
        relationship String)),
    `parent_id` String,
    `parent_name` String,
    `raw_data` String
)
ENGINE = MergeTree
ORDER BY (display_name, id)
SETTINGS index_granularity = 8192
```

## Tabla: `keywords`
```sql
CREATE TABLE rag.keywords
(
    `id` String,
    `raw_data` String,
    `display_name` String DEFAULT JSONExtractString(raw_data, 'display_name')
)
ENGINE = ReplacingMergeTree
ORDER BY id
SETTINGS index_granularity = 8192
```

## Tabla: `languages`
```sql
CREATE TABLE rag.languages
(
    `id` String,
    `raw_data` String,
    `display_name` String DEFAULT JSONExtractString(raw_data, 'display_name')
)
ENGINE = ReplacingMergeTree
ORDER BY id
SETTINGS index_granularity = 8192
```

## Tabla: `licenses`
```sql
CREATE TABLE rag.licenses
(
    `id` String,
    `raw_data` String,
    `display_name` String DEFAULT JSONExtractString(raw_data, 'display_name')
)
ENGINE = ReplacingMergeTree
ORDER BY id
SETTINGS index_granularity = 8192
```

## Tabla: `paper_author_map`
```sql
CREATE TABLE rag.paper_author_map
(
    `paper_id` String,
    `academic_name` String,
    `cvu` String,
    `orcid` String,
    `openalex_id` String,
    `institution` String,
    `institution_ror` String,
    `dependency` String,
    `dependency_id` String,
    `subdependency` String,
    `subdependency_id` String,
    `paper_title` String,
    `paper_year` UInt16,
    `citations` UInt32,
    `is_wos` UInt8,
    `is_scopus` UInt8,
    `is_pubmed` UInt8,
    `is_openalex` UInt8,
    `is_doaj` UInt8,
    `is_semantic_scholar` UInt8,
    `is_dimensions` UInt8,
    `is_lens` UInt8,
    `is_snii` UInt8,
    `ODS` Array(String),
    `source` String,
    `audit_verdict` String
)
ENGINE = ReplacingMergeTree
ORDER BY (institution, paper_id, cvu)
SETTINGS index_granularity = 8192
```

## Tabla: `paper_author_map_meta`
```sql
CREATE TABLE rag.paper_author_map_meta
(
    `sync_ts` DateTime DEFAULT now(),
    `mode` String,
    `rows_synced` UInt64,
    `ok` UInt8
)
ENGINE = MergeTree
ORDER BY sync_ts
SETTINGS index_granularity = 8192
```

## Tabla: `paper_entity_map`
```sql
CREATE TABLE rag.paper_entity_map
(
    `paper_id` String,
    `institution` String,
    `institution_ror` String,
    `dependency` String,
    `dependency_id` String,
    `subdependency` String,
    `subdependency_id` String,
    `paper_title` String,
    `paper_year` UInt16,
    `citations` UInt32,
    `is_wos` UInt8,
    `is_scopus` UInt8,
    `is_openalex` UInt8,
    `is_dimensions` UInt8,
    `is_semantic_scholar` UInt8,
    `is_pubmed` UInt8,
    `is_doaj` UInt8,
    `is_lens` UInt8,
    `source` String
)
ENGINE = ReplacingMergeTree
ORDER BY (institution_ror, paper_id, dependency_id, subdependency_id)
SETTINGS index_granularity = 8192
```

## Tabla: `publishers`
```sql
CREATE TABLE rag.publishers
(
    `id` String,
    `raw_data` String,
    `display_name` String DEFAULT JSONExtractString(raw_data, 'display_name'),
    `ror` String DEFAULT JSONExtractString(raw_data, 'ror')
)
ENGINE = ReplacingMergeTree
ORDER BY id
SETTINGS index_granularity = 8192
```

## Tabla: `sdgs`
```sql
CREATE TABLE rag.sdgs
(
    `id` String,
    `raw_data` String,
    `display_name` String DEFAULT JSONExtractString(raw_data, 'display_name')
)
ENGINE = ReplacingMergeTree
ORDER BY id
SETTINGS index_granularity = 8192
```

## Tabla: `source-types`
*No se pudo obtener el esquema: Received ClickHouse exception, code: 62, server response: Code: 62. DB::Exception: Syntax error: failed at position 29 (-) (line 1, col 29): -types
 FORMAT Native. Expected one of: INTO OUTFILE, FORMAT, SETTINGS, ParallelWithClause, PARALLEL WITH, end of query. (SYNTAX_ERROR) (version 25.4.5.24 (official build)) (for url http://10.90.0.87:8124)*

## Tabla: `sources`
```sql
CREATE TABLE rag.sources
(
    `id` String,
    `raw_data` String,
    `display_name` String DEFAULT JSONExtractString(raw_data, 'display_name'),
    `issn_l` String DEFAULT JSONExtractString(raw_data, 'issn_l'),
    `type` String DEFAULT JSONExtractString(raw_data, 'type'),
    `works_count` Int64 DEFAULT JSONExtractInt(raw_data, 'works_count'),
    `cited_by_count` Int64 DEFAULT JSONExtractInt(raw_data, 'cited_by_count'),
    `updated_date` String DEFAULT JSONExtractString(raw_data, 'updated_date'),
    `country_code` String DEFAULT JSONExtractString(raw_data, 'country_code'),
    INDEX issn_l_idx issn_l TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX source_name_idx display_name TYPE ngrambf_v1(4, 1024, 2, 1) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192
```

## Tabla: `subfields`
```sql
CREATE TABLE rag.subfields
(
    `id` String,
    `raw_data` String
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192
```

## Tabla: `summing_subfield_inst_metrics`
```sql
CREATE TABLE rag.summing_subfield_inst_metrics
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
SETTINGS index_granularity = 8192
```

## Tabla: `summing_subfield_metrics`
```sql
CREATE TABLE rag.summing_subfield_metrics
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
SETTINGS index_granularity = 8192
```

## Tabla: `tmp_academic_apc`
```sql
CREATE TABLE rag.tmp_academic_apc
(
    `id` String,
    `apc_paid` Float64,
    `apc_list` Float64
)
ENGINE = Join(ANY, LEFT, id)
```

## Tabla: `tmp_apc_2006_4`
```sql
CREATE TABLE rag.tmp_apc_2006_4
(
    `id` String,
    `apc_paid` Float64,
    `apc_list` Float64
)
ENGINE = Memory
```

## Tabla: `tmp_work_embs`
```sql
CREATE TABLE rag.tmp_work_embs
(
    `id` String,
    `specter` Array(Float32),
    `fastrp` Array(Float32)
)
ENGINE = Memory
```

## Tabla: `topics`
```sql
CREATE TABLE rag.topics
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
SETTINGS index_granularity = 8192
```

## Tabla: `work-types`
*No se pudo obtener el esquema: Received ClickHouse exception, code: 62, server response: Code: 62. DB::Exception: Syntax error: failed at position 27 (-) (line 1, col 27): -types
 FORMAT Native. Expected one of: INTO OUTFILE, FORMAT, SETTINGS, ParallelWithClause, PARALLEL WITH, end of query. (SYNTAX_ERROR) (version 25.4.5.24 (official build)) (for url http://10.90.0.87:8124)*

## Tabla: `works`
```sql
CREATE TABLE rag.works
(
    `id` String,
    `raw_data` String,
    `doi` String DEFAULT JSONExtractString(raw_data, 'doi'),
    `title` String DEFAULT JSONExtractString(raw_data, 'title'),
    `publication_year` Int32 DEFAULT JSONExtractInt(raw_data, 'publication_year'),
    `cited_by_count` Int64 DEFAULT JSONExtractInt(raw_data, 'cited_by_count'),
    `is_oa` String DEFAULT JSONExtractString(raw_data, 'open_access', 'is_oa'),
    `type` String DEFAULT JSONExtractString(raw_data, 'type'),
    `updated_date` String DEFAULT JSONExtractString(raw_data, 'updated_date'),
    `is_xpac` String DEFAULT JSONExtractString(raw_data, 'is_xpac'),
    `source_id` String DEFAULT JSONExtractString(raw_data, 'primary_location', 'source', 'id'),
    `author_names` Array(String) DEFAULT arrayDistinct(arrayFlatten(arrayMap(x -> [(x.1).1, x.2], JSONExtract(raw_data, 'authorships', 'Array(Tuple(author Tuple(display_name String), raw_author_name String))')))),
    `institution_rors` Array(String) DEFAULT arrayDistinct(arrayFlatten(arrayMap(x -> arrayMap(i -> (i.1), x.1), JSONExtract(raw_data, 'authorships', 'Array(Tuple(institutions Array(Tuple(ror String))))')))),
    `institution_names` Array(String) DEFAULT arrayDistinct(arrayFlatten(arrayMap(x -> arrayMap(i -> (i.2), x.1), JSONExtract(raw_data, 'authorships', 'Array(Tuple(institutions Array(Tuple(String, String))))')))),
    `primary_topic_id` String DEFAULT JSONExtractString(raw_data, 'primary_topic', 'id'),
    `institution_ids` Array(String) DEFAULT arrayDistinct(arrayFlatten(arrayMap(x -> arrayMap(i -> (i.3), x.1), JSONExtract(raw_data, 'authorships', 'Array(Tuple(institutions Array(Tuple(String, String, String))))')))),
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
    `source_type` String DEFAULT JSONExtractString(raw_data, 'primary_location', 'source', 'type'),
    `sdg_ids` Array(String) DEFAULT arrayMap(x -> (x.1), arrayFilter(x -> ((x.3) >= 0.4), JSONExtract(raw_data, 'sustainable_development_goals', 'Array(Tuple(String, String, Float32))'))),
    `awards` Array(String) DEFAULT arrayMap(x -> (x.3), JSONExtract(raw_data, 'grants', 'Array(Tuple(String, String, String))')),
    `concept_ids` Array(String) DEFAULT arrayMap(x -> (x.1), JSONExtract(raw_data, 'concepts', 'Array(Tuple(String, String, String, Int8, Float32))')),
    `all_country_codes` Array(String) DEFAULT arrayDistinct(arrayFlatten(arrayMap(x -> arrayMap(i -> (i.1), x.1), JSONExtract(raw_data, 'authorships', 'Array(Tuple(institutions Array(Tuple(String))))')))),
    `openalex_institution_ids` Array(String) MATERIALIZED arrayMap(x -> JSONExtractString(x, 'id'), JSONExtractArrayRaw(raw_data, 'institutions')),
    `author_ids` Array(String) MATERIALIZED arrayMap(x -> JSONExtractString(x, 'author', 'id'), JSONExtractArrayRaw(raw_data, 'authorships')),
    `topic_ids` Array(String) MATERIALIZED tupleElement(JSONExtract(raw_data, 'topics', 'Array(Tuple(id String))'), 'id'),
    `primary_subfield_id` String MATERIALIZED JSONExtractString(raw_data, 'primary_topic', 'subfield', 'id'),
    `primary_field_id` String MATERIALIZED JSONExtractString(raw_data, 'primary_topic', 'field', 'id'),
    `primary_domain_id` String MATERIALIZED JSONExtractString(raw_data, 'primary_topic', 'domain', 'id'),
    INDEX doi_idx doi TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX source_id_idx source_id TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX author_names_idx author_names TYPE tokenbf_v1(512, 3, 0) GRANULARITY 1,
    INDEX inst_rors_idx institution_rors TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX inst_names_idx institution_names TYPE tokenbf_v1(512, 3, 0) GRANULARITY 1,
    INDEX idx_title title TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 1,
    INDEX subfield_idx subfield TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX sdg_idx sdg_ids TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX concept_idx concept_ids TYPE tokenbf_v1(512, 3, 0) GRANULARITY 1,
    INDEX country_idx all_country_codes TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_oa_inst_ids openalex_institution_ids TYPE bloom_filter(0.01) GRANULARITY 1,
    INDEX idx_author_ids author_ids TYPE bloom_filter(0.01) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192
```

## Tabla: `works_academic_all`
```sql
CREATE TABLE rag.works_academic_all
(
    `id` String,
    `raw_data` String,
    `doi` String,
    `title` String,
    `publication_year` Int32,
    `cited_by_count` Int64,
    `is_oa` String,
    `type` String,
    `updated_date` String,
    `is_xpac` String,
    `source_id` String,
    `author_names` Array(String),
    `institution_rors` Array(String),
    `institution_names` Array(String),
    `primary_topic_id` String,
    `institution_ids` Array(String),
    `subfield` String,
    `field` String,
    `domain` String,
    `topic` String,
    `language` String,
    `oa_status` String,
    `fwci` Float32,
    `percentile` Float32,
    `is_top_10` UInt8,
    `is_top_1` UInt8,
    `country_code` String,
    `source_type` String,
    `sdg_ids` Array(String),
    `awards` Array(String),
    `concept_ids` Array(String),
    `all_country_codes` Array(String),
    `apc_paid_usd` Float64,
    `apc_list_usd` Float64,
    `counts_by_year` String,
    `is_doaj_indexed` UInt8,
    `is_doaj_journal` UInt8,
    `is_core_journal` UInt8,
    `is_retracted` UInt8,
    `has_repository_fulltext` UInt8,
    `license` String,
    `referenced_works_count` UInt32,
    `keywords` Array(String),
    `sdgs` Array(String),
    `journal_is_in_doaj` UInt8,
    `journal_is_core` UInt8,
    `any_repository_has_fulltext` UInt8,
    `embedding_nomic` Array(Float32),
    `embedding_specter` Array(Float32),
    `embedding_fastrp` Array(Float32)
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192
```

## Tabla: `works_flat`
```sql
CREATE TABLE rag.works_flat
(
    `id` String,
    `doi` String,
    `title` String,
    `abstract` String,
    `publication_year` UInt16,
    `publication_date` Date,
    `type` LowCardinality(String),
    `language` LowCardinality(String),
    `cited_by_count` UInt32,
    `fwci` Float32,
    `percentile` Float32,
    `is_top_10` UInt8,
    `is_top_1` UInt8,
    `referenced_works_count` UInt32,
    `source_id` LowCardinality(String),
    `source_type` LowCardinality(String),
    `is_oa` UInt8,
    `oa_status` LowCardinality(String),
    `topic_id` LowCardinality(String),
    `subfield_id` LowCardinality(String),
    `subfield_name` LowCardinality(String),
    `field_name` LowCardinality(String),
    `domain_name` LowCardinality(String),
    `author_ids` Array(String),
    `institution_ids` Array(String),
    `institution_types` Array(LowCardinality(String)),
    `country_codes` Array(LowCardinality(String)),
    `referenced_works` Array(String),
    `concepts` Array(LowCardinality(String)),
    `pmid` String,
    `mag_id` String,
    `is_retracted` UInt8,
    `is_paratext` UInt8,
    `volume` String,
    `issue` String,
    `first_page` String,
    `last_page` String,
    `all_topics` Array(LowCardinality(String)),
    `keywords` Array(String),
    `mesh` Array(String),
    `funder_ids` Array(String),
    `funder_names` Array(String),
    `sdgs` Array(LowCardinality(String)),
    `raw_data` String,
    `updated_date` String,
    `is_xpac` String,
    `author_names` Array(String),
    `institution_rors` Array(String),
    `institution_names` Array(String),
    `primary_topic_id` String,
    `subfield` String,
    `field` String,
    `domain` String,
    `topic` String,
    `country_code` String,
    `sdg_ids` Array(String),
    `awards` Array(String),
    `concept_ids` Array(String),
    `all_country_codes` Array(String),
    `apc_paid_usd` Float64,
    `apc_list_usd` Float64,
    `counts_by_year` String,
    `is_doaj_indexed` UInt8,
    `is_doaj_journal` UInt8,
    `is_core_journal` UInt8,
    `has_repository_fulltext` UInt8,
    `license` String,
    `journal_is_in_doaj` UInt8,
    `journal_is_core` UInt8,
    `any_repository_has_fulltext` UInt8
)
ENGINE = ReplacingMergeTree
PARTITION BY publication_year
ORDER BY id
SETTINGS index_granularity = 8192
```

## Tabla: `works_seed_mexico`
```sql
CREATE TABLE rag.works_seed_mexico
(
    `id` String,
    `raw_data` String,
    `doi` String,
    `title` String,
    `publication_year` Int32,
    `cited_by_count` Int64,
    `is_oa` String,
    `type` String,
    `updated_date` String,
    `is_xpac` String,
    `source_id` String,
    `author_names` Array(String),
    `institution_rors` Array(String),
    `institution_names` Array(String),
    `primary_topic_id` String,
    `institution_ids` Array(String),
    `subfield` String,
    `field` String,
    `domain` String,
    `topic` String,
    `language` String,
    `oa_status` String,
    `fwci` Float32,
    `percentile` Float32,
    `is_top_10` UInt8,
    `is_top_1` UInt8,
    `country_code` String,
    `source_type` String,
    `sdg_ids` Array(String),
    `awards` Array(String),
    `concept_ids` Array(String),
    `all_country_codes` Array(String),
    `apc_paid_usd` Float64,
    `apc_list_usd` Float64,
    `counts_by_year` String,
    `is_doaj_indexed` UInt8,
    `is_doaj_journal` UInt8,
    `is_core_journal` UInt8,
    `is_retracted` UInt8,
    `has_repository_fulltext` UInt8,
    `license` String,
    `referenced_works_count` UInt32,
    `keywords` Array(String),
    `sdgs` Array(String),
    `journal_is_in_doaj` UInt8,
    `journal_is_core` UInt8,
    `any_repository_has_fulltext` UInt8
)
ENGINE = MergeTree
ORDER BY (publication_year, id)
SETTINGS index_granularity = 8192
```

