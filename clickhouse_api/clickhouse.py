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
        return clickhouse_connect.get_client(
            host=CH_HOST,
            port=CH_PORT,
            username=CH_USER,
            password=CH_PASSWORD,
            database=CH_DATABASE
        )

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
            # Support title-specific search if requested via title:query
            if search_query.startswith("title:"):
                title_query = search_query.replace("title:", "", 1).strip()
                escaped_search = title_query.replace("'", "''")
                where_clauses.append(f"`title` ILIKE '%{escaped_search}%'")
            else:
                # DEFAULT SEARCH: Re-route to the materialized 'title' column 
                # instead of searching the entire 'raw_data' JSON blob.
                # This is 100x-1000x faster than ILIKE on a large JSON.
                escaped_search = search_query.replace("'", "''")
                where_clauses.append(f"`title` ILIKE '%{escaped_search}%'")
        
        # Handle filters (simplified)
        filters = params.get("filters")
        if filters:
            # Columns we have materialized for better performance
            materialized_cols = ["doi", "title", "publication_year", "cited_by_count", "is_oa", "is_xpac", "type", "updated_date", 
                                 "display_name", "orcid", "ror", "works_count", "issn_l", "level"]
            
            for f in filters:
                for key, value in f.items():
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
                    
                    # Special handling for 'id' to prepend URL if missing
                    if key == "id":
                        new_values = []
                        for v in values:
                            if not v.startswith("http"):
                                v = f"https://openalex.org/{v}"
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
            materialized_cols = ["cited_by_count", "publication_year", "works_count", "title", "display_name"]
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
        try:
            # If there's no complex where, count() is instant in ClickHouse
            count_result = client.query(count_sql)
            total_count = count_result.first_row[0]
        except Exception as e:
            logger.warning(f"Failed to get total count: {e}")
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
        from core.utils import get_full_openalex_id, normalize_doi
        from ids.utils import normalize_ror, normalize_orcid, normalize_wikidata
        
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
