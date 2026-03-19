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
                    # Handle pipe-separated multiple values (OR)
                    values = str(value).split("|")
                    
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

                    if key == "id":
                        vals_str = ",".join([f"'{v}'" for v in values])
                        where_clauses.append(f"id IN ({vals_str})")
                    elif key in materialized_cols:
                        if len(values) > 1:
                            vals_str = ",".join([f"'{v}'" for v in values])
                            where_clauses.append(f"`{key}` IN ({vals_str})")
                        else:
                            where_clauses.append(f"`{key}` = '{value}'")
                    else:
                        # Map dots to JSONExtract path
                        path_parts = [f"'{p}'" for p in key.split(".")]
                        path_str = ", ".join(path_parts)
                        if len(values) > 1:
                            vals_str = ",".join([f"'{v}'" for v in values])
                            where_clauses.append(f"JSONExtractString(raw_data, {path_str}) IN ({vals_str})")
                        else:
                            where_clauses.append(f"JSONExtractString(raw_data, {path_str}) = '{value}'")

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
        
        where_clause = ""
        if id_type == "id":
            where_clause = f"id = '{id_value}'"
        elif id_type == "doi":
            # Using the new 'doi' column for 1000x better performance
            where_clause = f"doi = '{id_value}'"
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
