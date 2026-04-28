import json
import logging
import clickhouse_connect
from settings import CH_HOST, CH_PORT, CH_USER, CH_PASSWORD, CH_DATABASE

logger = logging.getLogger(__name__)

class AttrDict(dict):
    """
    A dict that allows attribute access. 
    Required for compatibility with marshmallow schemas that use getattr.
    """
    def __getattr__(self, name):
        if name in self:
            val = self[name]
            if isinstance(val, dict) and not isinstance(val, AttrDict):
                return AttrDict(val)
            if isinstance(val, list):
                return [AttrDict(i) if isinstance(i, dict) and not isinstance(i, AttrDict) else i for i in val]
            return val
        if name == 'meta':
            return AttrDict({'score': None})
        raise AttributeError(f"'AttrDict' object has no attribute '{name}'")

    def __setattr__(self, name, value):
        self[name] = value

    def __delattr__(self, name):
        if name in self:
            del self[name]
        else:
            raise AttributeError(f"'AttrDict' object has no attribute '{name}'")

class ClickHouseBackend:
    def get_client(self):
        # Auto-enable secure connection for port 8124
        is_secure = (CH_PORT == 8124)
        try:
            return clickhouse_connect.get_client(
                host=CH_HOST,
                port=CH_PORT,
                username=CH_USER,
                password=CH_PASSWORD,
                database=CH_DATABASE,
                secure=is_secure,
                verify=False
            )
        except Exception as e:
            # If SSL fails but it's port 8124, try without secure flag just in case
            if is_secure:
                return clickhouse_connect.get_client(
                    host=CH_HOST,
                    port=CH_PORT,
                    username=CH_USER,
                    password=CH_PASSWORD,
                    database=CH_DATABASE,
                    secure=False,
                    verify=False
                )
            raise e

    def query_entity(self, entity_name, params):
        client = self.get_client()
        table_name = f"`{entity_name}`"
        
        # Base query
        sql = f"SELECT raw_data FROM {table_name}"
        count_sql = f"SELECT count() FROM {table_name}"
        
        where_clauses = []
        
        # Handle search
        search_query = params.get("search")
        if search_query and search_query != '""':
            # Map search column based on entity
            search_col_map = {
                "works": "title",
                "authors": "display_name",
                "institutions": "display_name",
                "sources": "display_name",
                "publishers": "display_name",
                "funders": "display_name"
            }
            search_col = search_col_map.get(entity_name, "display_name")

            # Support title-specific search if requested via title:query
            if search_query.startswith("title:"):
                title_query = search_query.replace("title:", "", 1).strip()
                escaped_search = title_query.replace("'", "''")
                where_clauses.append(f"`title` ILIKE '%{escaped_search}%'")
            elif search_query.startswith("display_name:"):
                dn_query = search_query.replace("display_name:", "", 1).strip()
                escaped_search = dn_query.replace("'", "''")
                where_clauses.append(f"`display_name` ILIKE '%{escaped_search}%'")
            else:
                # DEFAULT SEARCH: Robust Token-Based Search
                # 1. Normalize query (commas and hyphens to spaces)
                normalized_query = search_query.replace(",", " ").replace("-", " ")
                # 2. Extract tokens (unique and longer than 1 char if possible)
                tokens = [t.strip() for t in normalized_query.split() if len(t.strip()) > 0]
                
                if tokens:
                    token_clauses = []
                    # Vowel expansion map for accent-insensitivity (using unicode escapes to avoid transit mangling)
                    vowel_map = {
                        'a': '[a\u00e1\u00e0\u00e2\u00e4]', 'e': '[e\u00e9\u00e8\u00ea\u00eb]', 
                        'i': '[i\u00ed\u00ec\u00ee\u00ef]', 'o': '[o\u00f3\u00f2\u00f4\u00f6]', 
                        'u': '[u\u00fa\u00f9\u00fb\u00fc]',
                        'A': '[A\u00c1\u00c0\u00c2\u00c4]', 'E': '[E\u00c9\u00c8\u00ca\u00cb]', 
                        'I': '[I\u00cd\u00cc\u00ce\u00cf]', 'O': '[O\u00d3\u00d2\u00d4\u00d6]', 
                        'U': '[U\u00da\u00d9\u00db\u00dc]'
                    }
                    
                    for t in tokens:
                        # 1. Transform token into a case-insensitive regex with vowel expansion
                        regex_token = ""
                        for char in t:
                            regex_token += vowel_map.get(char, char)
                        
                        # 2. Use ClickHouse match() for regex-based matching
                        # (?i) makes the regex case-insensitive (Hyperscan/re2 supported)
                        token_clauses.append(f"match(`{search_col}`, '(?i){regex_token}')")
                    
                    if len(token_clauses) > 1:
                        where_clauses.append("(" + " AND ".join(token_clauses) + ")")
                    else:
                        where_clauses.append(token_clauses[0])
                else:
                    # Fallback for empty/weird tokens
                    escaped_search = search_query.replace("'", "''")
                    where_clauses.append(f"`{search_col}` ILIKE '%{escaped_search}%'")
        
        # Handle filters (simplified)
        filters = params.get("filters", [])
        if filters:
            # Map OpenAlex filter names to ClickHouse materialized columns
            # Only map keys that are direct top-level columns in the target entity table
            filter_map = {
                "id": "id",
                "doi": "doi",
                "primary_location.source.id": "source_id",
                "institutions.ror": "institution_rors",
                "authorships.institutions.ror": "institution_rors",
                "institutions.id": "institution_ids",
                "authorships.institutions.id": "institution_ids",
                "authorships.author.id": "author_ids",
                "orcid": "orcid",
                "authorships.author.orcid": "orcid",
                "primary_topic.id": "primary_topic_id",
                "sustainable_development_goals.id": "sdg_ids",
                "topics.id": "topic_ids",
                "primary_topic.subfield.id": "primary_subfield_id",
                "primary_topic.field.id": "primary_field_id",
                "primary_topic.domain.id": "primary_domain_id"
            }
            # Keys that require raw_data LIKE search (nested array fields or deep objects)
            raw_data_like_keys = {
                "authorships.author.institution_id",
                "topics.subfield.id",
                "topics.field.id",
                "topics.domain.id"
            }
            
            # Columns we have materialized for better performance
            materialized_cols = ["doi", "title", "publication_year", "cited_by_count", "is_oa", "is_xpac", "type", "updated_date", 
                                 "display_name", "orcid", "ror", "works_count", "issn_l", "level", "source_id", 
                                 "primary_topic_id", "institution_ids", "author_ids",
                                 "author_names", "institution_rors", "institution_names", "country_code",
                                 "sdg_ids", "topic_ids", "primary_subfield_id", "primary_field_id", "primary_domain_id"]
            
            for f in filters:
                for key, value in f.items():
                    # Handle nested array filters with raw_data LIKE (inexact but functional)
                    if key in raw_data_like_keys:
                        # Extract just the ID part (e.g. '00kgetx37' from full ROR URL)
                        val_str = str(value)
                        escaped_val = val_str.replace("'", "''")
                        where_clauses.append(f"raw_data LIKE '%{escaped_val}%'")
                        continue

                    # Map top-level keys to materialized columns if applicable
                    if key in filter_map:
                        key = filter_map[key]

                    # Handle date aliases
                    op = "="
                    if key == "from_publication_date":
                        key = "publication_date"
                        op = ">="
                    elif key == "to_publication_date":
                        key = "publication_date"
                        op = "<="
                    elif key == "from_created_date":
                        key = "created_date"
                        op = ">="
                    elif key == "to_created_date":
                        key = "created_date"
                        op = "<="
                    
                    # Handle operators in value
                    val_str = str(value)
                    if val_str.startswith(">="):
                        op = ">="
                        value = val_str[2:]
                    elif val_str.startswith("<="):
                        op = "<="
                        value = val_str[2:]
                    elif val_str.startswith(">"):
                        op = ">"
                        value = val_str[1:]
                    elif val_str.startswith("<"):
                        op = "<"
                        value = val_str[1:]
                    elif val_str.startswith("!"):
                        op = "!="
                        value = val_str[1:]

                    # Handle pipe-separated multiple values (OR)
                    values = str(value).split("|")
                    
                    # Normalize IDs based on field key
                    is_oa_id_field = any(k in key for k in ["id", "source_id", "institution_id", "author_id", "topic_id"])
                    is_doi_field = "doi" in key
                    is_ror_field = "ror" in key

                    new_values = []
                    for v in values:
                        v = v.strip()
                        if not v.startswith("http"):
                            # Handle OpenAlex IDs: W (Works), A (Authors), S (Sources), I (Institutions), T (Topics), C (Concepts)
                            if is_oa_id_field and any(v.upper().startswith(p) for p in ["S", "I", "A", "W", "T", "C"]) and len(v) > 1 and v[1:].isdigit():
                                v = f"https://openalex.org/{v}"
                            elif is_doi_field:
                                v = f"https://doi.org/{v}"
                            elif is_ror_field:
                                if v.startswith("ror.org/"):
                                    v = f"https://{v}"
                                else:
                                    v = f"https://ror.org/{v}"
                            elif "orcid" in key:
                                if not v.startswith("orcid.org/"):
                                    v = f"https://orcid.org/{v}"
                                elif v.startswith("orcid.org/"):
                                    v = f"https://{v}"
                        new_values.append(v)
                    values = new_values

                    # Handle search fields in filters (e.g., title.search)
                    if key.endswith(".search"):
                        field_name = key.replace(".search", "")
                        if field_name in materialized_cols:
                            escaped_val = str(value).replace("'", "''")
                            where_clauses.append(f"`{field_name}` ILIKE '%{escaped_val}%'")
                        else:
                            path_parts = [f"'{p}'" for p in field_name.split(".")]
                            path_str = ", ".join(path_parts)
                            escaped_val = str(value).replace("'", "''")
                            where_clauses.append(f"JSONExtractString(raw_data, {path_str}) ILIKE '%{escaped_val}%'")
                        continue

                    # Construct the SQL clause
                    if key in materialized_cols or key == "id":
                        col_name = f"`{key}`" if key != "id" else "id"
                        
                        # Handle array columns specifically
                        array_cols = ["author_names", "institution_rors", "institution_names", "institution_ids", "author_ids", "sdg_ids", "topic_ids"]
                        if key in array_cols:
                            if op == "=":
                                if len(values) > 1:
                                    vals_str = ",".join([f"'{v}'" for v in values])
                                    where_clauses.append(f"hasAny({col_name}, [{vals_str}])")
                                else:
                                    escaped_v = str(values[0]).replace("'", "''")
                                    where_clauses.append(f"has({col_name}, '{escaped_v}')")
                            else:
                                # For partial matches or other operators in arrays
                                clauses = []
                                for v in values:
                                    escaped_v = str(v).replace("'", "''")
                                    # Use ILIKE for partial array matching
                                    clauses.append(f"arrayExists(x -> x ILIKE '%{escaped_v}%', {col_name})")
                                if len(clauses) > 1:
                                    where_clauses.append("(" + " OR ".join(clauses) + ")")
                                else:
                                    where_clauses.append(clauses[0])
                            continue

                        if len(values) > 1 and op == "=":
                            vals_str = ",".join([f"'{v}'" for v in values])
                            where_clauses.append(f"{col_name} IN ({vals_str})")
                        elif len(values) > 1 and op == "!=":
                            vals_str = ",".join([f"'{v}'" for v in values])
                            where_clauses.append(f"{col_name} NOT IN ({vals_str})")
                        else:
                            # Apply operator to all values joined by OR if multiple values
                            clauses = []
                            for v in values:
                                escaped_v = str(v).replace("'", "''")
                                # Try to determine if it's numeric for better performance
                                if escaped_v.isdigit() or (escaped_v.startswith("-") and escaped_v[1:].isdigit()):
                                    clauses.append(f"{col_name} {op} {escaped_v}")
                                else:
                                    clauses.append(f"{col_name} {op} '{escaped_v}'")
                            if len(clauses) > 1:
                                where_clauses.append("(" + " OR ".join(clauses) + ")")
                            else:
                                where_clauses.append(clauses[0])
                    else:
                        # Fallback to JSON extraction
                        path_parts = [f"'{p}'" for p in key.split(".")]
                        path_str = ", ".join(path_parts)
                        # Detection for numeric vs string in JSON
                        extract_func = "JSONExtractString"
                        if key in ["publication_year", "cited_by_count", "works_count"]:
                            extract_func = "JSONExtractInt"
                        
                        clauses = []
                        for v in values:
                            escaped_v = str(v).replace("'", "''")
                            if extract_func == "JSONExtractInt":
                                clauses.append(f"{extract_func}(raw_data, {path_str}) {op} {escaped_v}")
                            else:
                                clauses.append(f"{extract_func}(raw_data, {path_str}) {op} '{escaped_v}'")
                        
                        if len(clauses) > 1:
                            where_clauses.append("(" + " OR ".join(clauses) + ")")
                        else:
                            where_clauses.append(clauses[0])


        if where_clauses:
            where_part = " WHERE " + " AND ".join(where_clauses)
            sql += where_part
            count_sql += where_part

        # Handle sorting
        sort = params.get("sort")
        if sort:
            # Columns we have materialized for better performance
            materialized_cols = ["cited_by_count", "publication_year", "works_count", "title", "display_name", "source_id", "country_code"]
            sort_clauses = []
            for key, order in sort.items():
                if key in materialized_cols:
                    sort_clauses.append(f"`{key}` {order.upper()}")
                else:
                    path_parts = [f"'{p}'" for p in key.split(".")]
                    path_str = ", ".join(path_parts)
                    # Simple heuristic for numeric fields
                    if "count" in key or "year" in key or "score" in key:
                        sort_clauses.append(f"JSONExtractFloat(raw_data, {path_str}) {order.upper()}")
                    else:
                        sort_clauses.append(f"JSONExtractString(raw_data, {path_str}) {order.upper()}")
            if sort_clauses:
                sql += " ORDER BY " + ", ".join(sort_clauses)

        # Handle pagination
        per_page = int(params.get("per_page", 25))
        page = int(params.get("page", 1))
        offset = (page - 1) * per_page
        sql += f" LIMIT {per_page} OFFSET {offset}"

        print(f"Executing ClickHouse SQL: {sql}")
        logger.info(f"Executing ClickHouse SQL: {sql}")
        result = client.query(sql)
        
        # Optional: query for total count (might be slow)
        total_count = 0
        skip_count = params.get("skip_count") == "true" or params.get("per_page") == "1"
        
        if not skip_count:
            try:
                # If there's no complex where, count() is instant in ClickHouse
                count_result = client.query(count_sql)
                total_count = count_result.first_row[0]
            except Exception as e:
                logger.warning(f"Failed to get total count: {e}")
                total_count = len(result.result_rows)
        else:
            total_count = len(result.result_rows)

        records = []
        for row in result.result_rows:
            data = json.loads(row[0])
            records.append(AttrDict(data))
            
        return AttrDict({
            "results": records,
            "meta": {
                "count": total_count, 
                "db_response_time_ms": 0,
                "page": page,
                "per_page": per_page
            }
        })

    def get_item_by_id(self, entity_name, id_value, id_type="id"):
        client = self.get_client()
        table_name = f"`{entity_name}`"
        from core.utils import get_full_openalex_id
        from ids.utils import normalize_ror, normalize_orcid, normalize_wikidata, normalize_doi
        
        where_clause = ""
        if id_type == "id":
            full_id = get_full_openalex_id(id_value)
            if full_id:
                id_value = full_id
            elif not id_value.startswith("http"):
                id_value = f"https://openalex.org/{id_value}"
            where_clause = f"id = '{id_value}'"
        elif id_type == "doi":
            clean_doi = normalize_doi(id_value, return_none_if_error=True)
            if clean_doi:
                id_value = f"https://doi.org/{clean_doi}"
            where_clause = f"doi = '{id_value}'"
        elif id_type == "ror":
            clean_ror = normalize_ror(id_value)
            if clean_ror:
                id_value = f"https://ror.org/{clean_ror}"
            where_clause = f"ror = '{id_value}'"
        elif id_type == "orcid":
            clean_orcid = normalize_orcid(id_value)
            if clean_orcid:
                id_value = f"https://orcid.org/{clean_orcid}"
            where_clause = f"orcid = '{id_value}'"
        elif id_type == "wikidata":
            clean_wiki = normalize_wikidata(id_value)
            if clean_wiki:
                id_value = f"https://www.wikidata.org/wiki/{clean_wiki}"
            where_clause = f"JSONExtractString(raw_data, 'ids', 'wikidata') = '{id_value}'"
        elif id_type == "pmid":
             where_clause = f"JSONExtractString(raw_data, 'ids', 'pmid') = '{id_value}'"
        else:
            # Fallback to general ID lookup in ids object
            where_clause = f"JSONExtractString(raw_data, 'ids', '{id_type}') = '{id_value}'"

        sql = f"SELECT raw_data FROM {table_name} WHERE {where_clause} LIMIT 1"
        
        print(f"Executing ClickHouse SQL (GetByID): {sql}")
        logger.info(f"Executing ClickHouse SQL (GetByID): {sql}")
        result = client.query(sql)
        
        if not result.result_rows:
            return None
            
        data = json.loads(result.result_rows[0][0])
        return AttrDict(data)

def get_clickhouse_backend():
    return ClickHouseBackend()
