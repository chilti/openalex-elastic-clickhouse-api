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
            return AttrDict()
        return None

    def __setattr__(self, name, value):
        self[name] = value

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
        
        where_clauses = []
        
        # Handle search
        search_query = params.get("search")
        if search_query and search_query != '""':
            # Basic text search on raw_data string (very naive first pass)
            escaped_search = search_query.replace("'", "''")
            where_clauses.append(f"raw_data LIKE '%{escaped_search}%'")
        
        # Handle filters (simplified)
        filters = params.get("filters")
        if filters:
            for f in filters:
                for key, value in f.items():
                    # Handle pipe-separated multiple values (OR)
                    values = value.split("|")
                    if key == "id":
                        vals_str = ",".join([f"'{v}'" for v in values])
                        where_clauses.append(f"id IN ({vals_str})")
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
            sql += " WHERE " + " AND ".join(where_clauses)

        # Handle sorting
        sort = params.get("sort")
        if sort:
            sort_clauses = []
            for key, order in sort.items():
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
        
        records = []
        for row in result.result_rows:
            data = json.loads(row[0])
            records.append(AttrDict(data))
            
        return AttrDict({
            "results": records,
            "meta": {
                "count": len(records), 
                "db_response_time_ms": 0,
                "page": page,
                "per_page": per_page
            }
        })

def get_clickhouse_backend():
    return ClickHouseBackend()
