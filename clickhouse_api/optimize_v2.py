import os
import logging
import clickhouse_connect
from dotenv import load_dotenv

# Load from existing .env in same directory
env_path = os.path.join(os.path.dirname(__file__), '.env')
load_dotenv(env_path)

CH_HOST = os.environ.get('CH_HOST', '10.90.0.87')
CH_PORT = int(os.environ.get('CH_PORT', 8124))
CH_USER = os.environ.get('CH_USER', 'default')
CH_PASSWORD = os.environ.get('CH_PASSWORD', '')
CH_DATABASE = os.environ.get('CH_DATABASE', 'rag')

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def optimize():
    try:
        is_secure = (CH_PORT == 8124)
        client = clickhouse_connect.get_client(
            host=CH_HOST,
            port=CH_PORT,
            username=CH_USER,
            password=CH_PASSWORD,
            database=CH_DATABASE,
            secure=is_secure,
            verify=False
        )
        logger.info(f"Connected to ClickHouse: {CH_HOST}")
    except Exception as e:
        logger.error(f"Could not connect to ClickHouse: {e}")
        return

    # Map of (Entity -> Columns to materialize from JSON)
    optimizations = {
        "works": [
            ("doi", "String"),
            ("title", "String"),
            ("publication_year", "Int32", "JSONExtractInt(raw_data, 'publication_year')"),
            ("cited_by_count", "Int64", "JSONExtractInt(raw_data, 'cited_by_count')"),
            ("is_oa", "String", "JSONExtractString(raw_data, 'open_access', 'is_oa')"),
            ("is_xpac", "String", "JSONExtractString(raw_data, 'is_xpac')"),
            ("type", "String"),
            ("source_id", "String", "JSONExtractString(raw_data, 'primary_location', 'source', 'id')"),
            ("primary_topic_id", "String", "JSONExtractString(raw_data, 'primary_topic', 'id')"),
            ("institution_ids", "Array(String)", "arrayDistinct(arrayFlatten(arrayMap(x -> arrayMap(i -> i.1, x.1), JSONExtract(raw_data, 'authorships', 'Array(Tuple(institutions Array(Tuple(String, String, String))))'))))"),
            ("author_names", "Array(String)", "arrayDistinct(arrayFlatten(arrayMap(x -> [x.1.1, x.2], JSONExtract(raw_data, 'authorships', 'Array(Tuple(author Tuple(display_name String), raw_author_name String))'))))"),
            ("institution_rors", "Array(String)", "arrayDistinct(arrayFlatten(arrayMap(x -> arrayMap(i -> i.1, x.1), JSONExtract(raw_data, 'authorships', 'Array(Tuple(institutions Array(Tuple(String, String, String))))'))))"),
            ("institution_names", "Array(String)", "arrayDistinct(arrayFlatten(arrayMap(x -> arrayMap(i -> i.2, x.1), JSONExtract(raw_data, 'authorships', 'Array(Tuple(institutions Array(Tuple(String, String, String))))'))))"),
            ("updated_date", "String")
        ],
        "institutions": [
            ("ror", "String"),
            ("display_name", "String"),
            ("country_code", "String"),
            ("type", "String"),
            ("works_count", "Int32", "JSONExtractInt(raw_data, 'works_count')"),
            ("cited_by_count", "Int64", "JSONExtractInt(raw_data, 'cited_by_count')"),
            ("updated_date", "String")
        ],
        "authors": [
            ("orcid", "String"),
            ("display_name", "String"),
            ("works_count", "Int32", "JSONExtractInt(raw_data, 'works_count')"),
            ("cited_by_count", "Int64", "JSONExtractInt(raw_data, 'cited_by_count')"),
            ("updated_date", "String")
        ],
        "sources": [
            ("display_name", "String"),
            ("issn_l", "String"),
            ("type", "String"),
            ("country_code", "String"),
            ("works_count", "Int32", "JSONExtractInt(raw_data, 'works_count')"),
            ("cited_by_count", "Int64", "JSONExtractInt(raw_data, 'cited_by_count')"),
            ("updated_date", "String")
        ]
    }

    # Get list of tables in DB
    tables_result = client.query("SHOW TABLES")
    existing_tables = [row[0] for row in tables_result.result_rows]
    logger.info(f"Existing tables: {existing_tables}")

    for table, columns in optimizations.items():
        if table not in existing_tables:
            logger.warning(f"Skipping optimizations for table '{table}' as it does not exist.")
            continue
            
        logger.info(f"Optimizing table: {table}")
        
        for col_data in columns:
            col_name = col_data[0]
            col_type = col_data[1]
            col_expr = col_data[2] if len(col_data) > 2 else f"JSONExtractString(raw_data, '{col_name}')"
            
            # 1. Add column
            try:
                logger.info(f"  Adding/Checking column '{col_name}' in '{table}'...")
                client.command(f"ALTER TABLE `{table}` ADD COLUMN IF NOT EXISTS `{col_name}` {col_type} DEFAULT {col_expr}")
            except Exception as e:
                logger.error(f"  Failed to add column '{col_name}' to '{table}': {e}")
                continue

            # 2. Materialize column (populates existing rows)
            try:
                logger.info(f"  Materializing column '{col_name}' in '{table}'...")
                client.command(f"ALTER TABLE `{table}` MATERIALIZE COLUMN `{col_name}`")
            except Exception as e:
                logger.error(f"  Failed to materialize column '{col_name}' in '{table}': {e}")

    # 3. Add Skip Indexes for performance
    indices = {
        "works": [
            ("doi_idx", "doi", "bloom_filter(0.01)", "1"),
            ("source_id_idx", "source_id", "bloom_filter(0.01)", "1"),
            ("author_names_idx", "author_names", "tokenbf_v1(512, 3, 0)", "1"),
            ("inst_rors_idx", "institution_rors", "bloom_filter(0.01)", "1"),
            ("inst_names_idx", "institution_names", "tokenbf_v1(512, 3, 0)", "1")
        ],
        "institutions": [
            ("ror_idx", "ror", "bloom_filter(0.01)", "1"),
            ("inst_name_idx", "display_name", "ngrambf_v1(4, 1024, 2, 1)", "1")
        ],
        "authors": [
            ("auth_name_idx", "display_name", "ngrambf_v1(4, 1024, 2, 1)", "1")
        ],
        "sources": [
            ("issn_l_idx", "issn_l", "bloom_filter(0.01)", "1"),
            ("source_name_idx", "display_name", "ngrambf_v1(4, 1024, 2, 1)", "1")
        ]
    }
    
    for table, table_indices in indices.items():
        if table not in existing_tables: continue
        for idx_name, col_name, idx_type, granularity in table_indices:
            try:
                logger.info(f"  Adding index '{idx_name}' on '{col_name}' in '{table}'...")
                client.command(f"ALTER TABLE `{table}` ADD INDEX IF NOT EXISTS `{idx_name}` `{col_name}` TYPE {idx_type} GRANULARITY {granularity}")
                logger.info(f"  Materializing index '{idx_name}' in '{table}'...")
                client.command(f"ALTER TABLE `{table}` MATERIALIZE INDEX `{idx_name}`")
            except Exception as e:
                logger.error(f"  Failed to add index '{idx_name}' to '{table}': {e}")

    logger.info("Optimization process complete!")

if __name__ == "__main__":
    optimize()
