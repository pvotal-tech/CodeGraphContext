# src/codegraphcontext/core/database_spanner.py
import os
import re
import json
import uuid
import threading
from typing import Optional, Tuple, Dict, Any, List

from codegraphcontext.utils.debug_log import debug_log, info_logger, error_logger, warning_logger

NODE_TABLES = {
    "Repository": {"schema": "path STRING(MAX) NOT NULL, name STRING(MAX), is_dependency BOOL, properties JSON, last_indexed_commit STRING(MAX)", "pk": "path"},
    "File": {"schema": "path STRING(MAX) NOT NULL, name STRING(MAX), relative_path STRING(MAX), is_dependency BOOL, properties JSON", "pk": "path"},
    "Directory": {"schema": "path STRING(MAX) NOT NULL, name STRING(MAX), properties JSON", "pk": "path"},
    "Module": {"schema": "name STRING(MAX) NOT NULL, lang STRING(MAX), full_import_name STRING(MAX), properties JSON", "pk": "name"},
    "Function": {"schema": "uid STRING(MAX) NOT NULL, name STRING(MAX), path STRING(MAX), line_number INT64, end_line INT64, source STRING(MAX), docstring STRING(MAX), lang STRING(MAX), cyclomatic_complexity INT64, is_dependency BOOL, properties JSON, decorators ARRAY<STRING(MAX)> AS (JSON_VALUE_ARRAY(properties, '$.decorators')) STORED, args ARRAY<STRING(MAX)> AS (JSON_VALUE_ARRAY(properties, '$.args')) STORED, context STRING(MAX) AS (JSON_VALUE(properties, '$.context')) STORED, context_type STRING(MAX) AS (JSON_VALUE(properties, '$.context_type')) STORED, class_context STRING(MAX) AS (JSON_VALUE(properties, '$.class_context')) STORED", "pk": "uid"},
    "Class": {"schema": "uid STRING(MAX) NOT NULL, name STRING(MAX), path STRING(MAX), line_number INT64, end_line INT64, source STRING(MAX), docstring STRING(MAX), lang STRING(MAX), is_dependency BOOL, properties JSON, decorators ARRAY<STRING(MAX)> AS (JSON_VALUE_ARRAY(properties, '$.decorators')) STORED, context STRING(MAX) AS (JSON_VALUE(properties, '$.context')) STORED, context_type STRING(MAX) AS (JSON_VALUE(properties, '$.context_type')) STORED, class_context STRING(MAX) AS (JSON_VALUE(properties, '$.class_context')) STORED", "pk": "uid"},
    "Variable": {"schema": "uid STRING(MAX) NOT NULL, name STRING(MAX), path STRING(MAX), line_number INT64, end_line INT64, source STRING(MAX), docstring STRING(MAX), lang STRING(MAX), value STRING(MAX), type STRING(MAX), is_dependency BOOL, properties JSON, decorators ARRAY<STRING(MAX)> AS (JSON_VALUE_ARRAY(properties, '$.decorators')) STORED, context STRING(MAX) AS (JSON_VALUE(properties, '$.context')) STORED, class_context STRING(MAX) AS (JSON_VALUE(properties, '$.class_context')) STORED", "pk": "uid"},
    "Trait": {"schema": "uid STRING(MAX) NOT NULL, name STRING(MAX), path STRING(MAX), line_number INT64, end_line INT64, source STRING(MAX), docstring STRING(MAX), lang STRING(MAX), is_dependency BOOL, properties JSON, decorators ARRAY<STRING(MAX)> AS (JSON_VALUE_ARRAY(properties, '$.decorators')) STORED, context STRING(MAX) AS (JSON_VALUE(properties, '$.context')) STORED, context_type STRING(MAX) AS (JSON_VALUE(properties, '$.context_type')) STORED, class_context STRING(MAX) AS (JSON_VALUE(properties, '$.class_context')) STORED", "pk": "uid"},
    "Interface": {"schema": "uid STRING(MAX) NOT NULL, name STRING(MAX), path STRING(MAX), line_number INT64, end_line INT64, source STRING(MAX), docstring STRING(MAX), lang STRING(MAX), is_dependency BOOL, properties JSON, decorators ARRAY<STRING(MAX)> AS (JSON_VALUE_ARRAY(properties, '$.decorators')) STORED, context STRING(MAX) AS (JSON_VALUE(properties, '$.context')) STORED, context_type STRING(MAX) AS (JSON_VALUE(properties, '$.context_type')) STORED, class_context STRING(MAX) AS (JSON_VALUE(properties, '$.class_context')) STORED", "pk": "uid"},
    "Macro": {"schema": "uid STRING(MAX) NOT NULL, name STRING(MAX), path STRING(MAX), line_number INT64, end_line INT64, source STRING(MAX), docstring STRING(MAX), lang STRING(MAX), is_dependency BOOL, properties JSON, decorators ARRAY<STRING(MAX)> AS (JSON_VALUE_ARRAY(properties, '$.decorators')) STORED, context STRING(MAX) AS (JSON_VALUE(properties, '$.context')) STORED, context_type STRING(MAX) AS (JSON_VALUE(properties, '$.context_type')) STORED, class_context STRING(MAX) AS (JSON_VALUE(properties, '$.class_context')) STORED", "pk": "uid"},
    "Struct": {"schema": "uid STRING(MAX) NOT NULL, name STRING(MAX), path STRING(MAX), line_number INT64, end_line INT64, source STRING(MAX), docstring STRING(MAX), lang STRING(MAX), is_dependency BOOL, properties JSON, decorators ARRAY<STRING(MAX)> AS (JSON_VALUE_ARRAY(properties, '$.decorators')) STORED, context STRING(MAX) AS (JSON_VALUE(properties, '$.context')) STORED, context_type STRING(MAX) AS (JSON_VALUE(properties, '$.context_type')) STORED, class_context STRING(MAX) AS (JSON_VALUE(properties, '$.class_context')) STORED", "pk": "uid"},
    "Enum": {"schema": "uid STRING(MAX) NOT NULL, name STRING(MAX), path STRING(MAX), line_number INT64, end_line INT64, source STRING(MAX), docstring STRING(MAX), lang STRING(MAX), is_dependency BOOL, properties JSON, decorators ARRAY<STRING(MAX)> AS (JSON_VALUE_ARRAY(properties, '$.decorators')) STORED, context STRING(MAX) AS (JSON_VALUE(properties, '$.context')) STORED, context_type STRING(MAX) AS (JSON_VALUE(properties, '$.context_type')) STORED, class_context STRING(MAX) AS (JSON_VALUE(properties, '$.class_context')) STORED", "pk": "uid"},
    "Union": {"schema": "uid STRING(MAX) NOT NULL, name STRING(MAX), path STRING(MAX), line_number INT64, end_line INT64, source STRING(MAX), docstring STRING(MAX), lang STRING(MAX), is_dependency BOOL, properties JSON, decorators ARRAY<STRING(MAX)> AS (JSON_VALUE_ARRAY(properties, '$.decorators')) STORED, context STRING(MAX) AS (JSON_VALUE(properties, '$.context')) STORED, context_type STRING(MAX) AS (JSON_VALUE(properties, '$.context_type')) STORED, class_context STRING(MAX) AS (JSON_VALUE(properties, '$.class_context')) STORED", "pk": "uid"},
    "Annotation": {"schema": "uid STRING(MAX) NOT NULL, name STRING(MAX), path STRING(MAX), line_number INT64, end_line INT64, source STRING(MAX), docstring STRING(MAX), lang STRING(MAX), is_dependency BOOL, properties JSON, decorators ARRAY<STRING(MAX)> AS (JSON_VALUE_ARRAY(properties, '$.decorators')) STORED, context STRING(MAX) AS (JSON_VALUE(properties, '$.context')) STORED, context_type STRING(MAX) AS (JSON_VALUE(properties, '$.context_type')) STORED, class_context STRING(MAX) AS (JSON_VALUE(properties, '$.class_context')) STORED", "pk": "uid"},
    "Record": {"schema": "uid STRING(MAX) NOT NULL, name STRING(MAX), path STRING(MAX), line_number INT64, end_line INT64, source STRING(MAX), docstring STRING(MAX), lang STRING(MAX), is_dependency BOOL, properties JSON, decorators ARRAY<STRING(MAX)> AS (JSON_VALUE_ARRAY(properties, '$.decorators')) STORED, context STRING(MAX) AS (JSON_VALUE(properties, '$.context')) STORED, context_type STRING(MAX) AS (JSON_VALUE(properties, '$.context_type')) STORED, class_context STRING(MAX) AS (JSON_VALUE(properties, '$.class_context')) STORED", "pk": "uid"},
    "Property": {"schema": "uid STRING(MAX) NOT NULL, name STRING(MAX), path STRING(MAX), line_number INT64, end_line INT64, source STRING(MAX), docstring STRING(MAX), lang STRING(MAX), is_dependency BOOL, properties JSON, decorators ARRAY<STRING(MAX)> AS (JSON_VALUE_ARRAY(properties, '$.decorators')) STORED, context STRING(MAX) AS (JSON_VALUE(properties, '$.context')) STORED, context_type STRING(MAX) AS (JSON_VALUE(properties, '$.context_type')) STORED, class_context STRING(MAX) AS (JSON_VALUE(properties, '$.class_context')) STORED", "pk": "uid"},
    "Parameter": {"schema": "uid STRING(MAX) NOT NULL, name STRING(MAX), path STRING(MAX), function_line_number INT64, properties JSON", "pk": "uid"},
}

EDGE_TABLES = {
    "CONTAINS": {
        "pairs": [
            ("File", "Function"), ("File", "Class"), ("File", "Variable"), 
            ("File", "Trait"), ("File", "Interface"), ("Macro", "Macro"), 
            ("File", "Macro"), ("File", "Struct"), ("File", "Enum"), 
            ("File", "Union"), ("File", "Annotation"), ("File", "Record"), 
            ("File", "Property"), ("Repository", "Directory"), 
            ("Directory", "Directory"), ("Directory", "File"), 
            ("Repository", "File"), ("Class", "Function"), ("Function", "Function")
        ],
        "properties": ", properties JSON"
    },
    "CALLS": {
        "pairs": [
            ("Function", "Function"), ("Function", "Class"), 
            ("File", "Function"), ("File", "Class"), 
            ("Class", "Function"), ("Class", "Class")
        ],
        "properties": ", line_number INT64, args ARRAY<STRING(MAX)>, full_call_name STRING(MAX), properties JSON"
    },
    "IMPORTS": {
        "pairs": [("File", "Module")],
        "properties": ", alias STRING(MAX), full_import_name STRING(MAX), imported_name STRING(MAX), line_number INT64, properties JSON"
    },
    "INHERITS": {
        "pairs": [("Class", "Class"), ("Record", "Record"), ("Interface", "Interface")],
        "properties": ", properties JSON"
    },
    "HAS_PARAMETER": {
        "pairs": [("Function", "Parameter")],
        "properties": ", properties JSON"
    },
    "INCLUDES": {
        "pairs": [("Class", "Module")],
        "properties": ", properties JSON"
    },
    "IMPLEMENTS": {
        "pairs": [("Class", "Interface"), ("Struct", "Interface"), ("Record", "Interface")],
        "properties": ", properties JSON"
    }
}

class SpannerDBManager:
    """Manages the Google Cloud Spanner database connection as a singleton."""
    _instance = None
    _client = None
    _instance_id = None
    _database_id = None
    _database_obj = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(SpannerDBManager, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if hasattr(self, '_initialized'):
            return

        self._instance_id = os.getenv('SPANNER_INSTANCE_ID', 'my-spanner-instance')
        self._database_id = os.getenv('SPANNER_DATABASE_ID', 'my-graph-database')
        self._graph_name = os.getenv('SPANNER_GRAPH_NAME', 'CodeGraph')
        
        self.name = 'spanner'
        self._initialized = True

    def get_driver(self):
        if self._client is None:
            with self._lock:
                if self._client is None:
                    try:
                        from google.cloud import spanner
                        import google.auth
                        
                        project_id = os.environ.get("GOOGLE_CLOUD_PROJECT", os.environ.get("GCP_PROJECT", None))
                        info_logger(f"Initializing Spanner at {self._instance_id}/{self._database_id} in {project_id}")
                        self._client = spanner.Client(project=project_id, disable_builtin_metrics=True)
                        
                        instance = self._client.instance(self._instance_id)
                        self._database_obj = instance.database(self._database_id)
                        
                        info_logger("Spanner connection established")
                    except ImportError:
                        error_logger("Google Cloud Spanner is not installed. Run 'pip install google-cloud-spanner'")
                        raise ValueError("google-cloud-spanner missing.")
                    except Exception as e:
                        error_logger(f"Failed to initialize Spanner: {e}")
                        raise

        return SpannerDriverWrapper(self._database_obj, self._graph_name)

    def provision_schema(self, console=None):
        """Creates Node tables, Edge tables, and the Property Graph via DDL if they don't exist."""
        if not self._database_obj:
            self.get_driver()

        def log(msg):
            if console: console.print(msg)
            else: info_logger(msg)

        log(f"[cyan]Provisioning Spanner Property Graph DDL for: [bold]{self._graph_name}[/bold][/cyan]")
        ddl_statements = []

        # 1. Create Node Tables
        for table_name, table_info in NODE_TABLES.items():
            ddl_statements.append(f"""
            CREATE TABLE IF NOT EXISTS `{table_name}` (
                {table_info['schema']}
            ) PRIMARY KEY ({table_info['pk']})
            """)

        # 2. Create physical Edge Tables
        for edge_table, table_info in EDGE_TABLES.items():
            # A universal physical table per edge type
            ddl_statements.append(f"""
            CREATE TABLE IF NOT EXISTS `{edge_table}` (
                id STRING(36) NOT NULL,
                src_id STRING(MAX) NOT NULL,
                dst_id STRING(MAX) NOT NULL{table_info['properties']}
            ) PRIMARY KEY (id)
            """)

        # 3. Create Property Graph
        # Spanner limits CREATE PROPERTY GRAPH to full replacement or initial creation, no IF NOT EXISTS.
        # Check if it exists via information schema first.
        
        # Build node tables clause
        node_tables_clause = ",\n".join([f"    `{node}`" for node in NODE_TABLES.keys()])
        
        # Build edge tables clause
        edge_tables_clauses = []
        for edge_label, edge_data in EDGE_TABLES.items():
            for src_node, dst_node in edge_data['pairs']:
                # The relationship creates a unique edge definition per pair mapped to the SAME physical table
                src_pk = NODE_TABLES[src_node]['pk']
                dst_pk = NODE_TABLES[dst_node]['pk']
                edge_tables_clauses.append(f"""
    `{edge_label}` AS `{edge_label}_{src_node}_{dst_node}`
      SOURCE KEY (src_id) REFERENCES `{src_node}` ({src_pk})
      DESTINATION KEY (dst_id) REFERENCES `{dst_node}` ({dst_pk})
      LABEL `{edge_label}`""")

        edge_tables_clause = ",\n".join(edge_tables_clauses)
        
        graph_ddl = f"""
        CREATE PROPERTY GRAPH `{self._graph_name}`
        NODE TABLES (
{node_tables_clause}
        )
        EDGE TABLES ({edge_tables_clause}
        )
        """

        log("[yellow]Updating DDL. This may take a few minutes...[/yellow]")
        try:
            # We wrap graph creation in a try-except to handle existing graphs, 
            # as Spanner does not support IF NOT EXISTS for CREATE PROPERTY GRAPH.
            # We first run the table creations.
            operation = self._database_obj.update_ddl(ddl_statements)
            operation.result(timeout=120)  # Wait for table creation DDL
            log("[green]✅ Underlying Tables provisioned.[/green]")
        except Exception as e:
            if "Duplicate name" in str(e) or "already exists" in str(e):
                log("[dim]Tables already exist.[/dim]")
            else:
                raise e

        try:
            # Then we run the graph creation
            operation = self._database_obj.update_ddl([graph_ddl])
            operation.result(timeout=120)
            log(f"[green]✅ Property Graph {self._graph_name} provisioned.[/green]")
        except Exception as e:
            if "Duplicate name" in str(e) or "already exists" in str(e):
                log(f"[dim]Property Graph {self._graph_name} already exists.[/dim]")
            else:
                error_logger(f"Failed to create property graph: {e}")
                log(f"[red]❌ Property Graph creation failed: {e}[/red]")

    def close_driver(self):
        if self._client is not None:
            info_logger("Closing Spanner DB connection")
            self._client = None

    def is_connected(self) -> bool:
        if self._client is None or self._database_obj is None: return False
        try:
            with self._database_obj.snapshot() as snapshot:
                snapshot.execute_sql("SELECT 1")
            return True
        except Exception:
            return False
            
    def get_backend_type(self) -> str:
        return 'spanner'

    @staticmethod
    def validate_config() -> Tuple[bool, Optional[str]]:
        if not os.getenv('SPANNER_INSTANCE_ID') or not os.getenv('SPANNER_DATABASE_ID'):
            return False, "SPANNER_INSTANCE_ID and SPANNER_DATABASE_ID must be set."
        return True, None

    @staticmethod
    def test_connection() -> Tuple[bool, Optional[str]]:
        try:
            from google.cloud import spanner
            client = spanner.Client(disable_builtin_metrics=True)
            instance = client.instance(os.getenv('SPANNER_INSTANCE_ID'))
            db = instance.database(os.getenv('SPANNER_DATABASE_ID'))
            with db.snapshot() as snapshot:
                snapshot.execute_sql("SELECT 1")
            return True, None
        except Exception as e:
            return False, f"Spanner connection failed: {e}"


class SpannerRecord:
    def __init__(self, data_dict):
        self._data = data_dict
        self._keys = list(data_dict.keys())
    
    def data(self): return self._data
    def keys(self): return self._keys
    def items(self): return self._data.items()
    def values(self): return list(self._data.values())
    def __len__(self): return len(self._data)
    
    def __getitem__(self, key):
        if isinstance(key, int):
            if 0 <= key < len(self._keys):
                return self._data[self._keys[key]]
            raise IndexError(f"Index {key} out of range")
        return self._data[key]
    
    def get(self, key, default=None):
        return self._data.get(key, default)


class SpannerResultWrapper:
    def __init__(self, result_list):
        self.result_list = result_list
        self._consumed = False
        
    def consume(self):
        self._consumed = True
        return self
        
    def single(self):
        return SpannerRecord(self.result_list[0]) if self.result_list else None
        
    def data_raw(self) -> List[Dict[str, Any]]:
        return self.result_list

    def data(self) -> List[Dict[str, Any]]:
        return self.data_raw()

    def __iter__(self):
        return iter([SpannerRecord(r) for r in self.result_list])

def _auto_alias_return_clause(query: str) -> Tuple[str, Dict[str, str]]:
    """
    Finds the final RETURN clause and ensures every expression has an AS alias (c_0, c_1).
    Returns (aliased_query, alias_map), where alias_map maps auto-generated
    aliases back to the original expression string to simulate Neo4j implicit aliasing parity.
    """
    last_return_pos = -1
    i = 0
    n = len(query)
    in_single = False
    in_double = False
    
    while i < n:
        c = query[i]
        if c == "'":
            if not in_double: in_single = not in_single
        elif c == '"':
            if not in_single: in_double = not in_double
        elif not in_single and not in_double:
            if query[i:i+7].upper() == "RETURN " and (i == 0 or query[i-1].isspace()):
                last_return_pos = i
        i += 1
        
    if last_return_pos == -1:
        return query, {}
        
    prefix = query[:last_return_pos + 7]
    remainder = query[last_return_pos + 7:]
    
    columns = []
    current_col = []
    
    paren_depth = 0
    bracket_depth = 0
    brace_depth = 0
    in_single = False
    in_double = False
    
    i = 0
    m = len(remainder)
    
    def check_keyword(idx, kw):
        if idx + len(kw) <= m:
            if remainder[idx:idx+len(kw)].upper() == kw:
                if idx + len(kw) == m or remainder[idx+len(kw)].isspace():
                    return True
        return False
        
    end_idx = m
    while i < m:
        c = remainder[i]
        
        if c == "'":
            if not in_double: in_single = not in_single
            current_col.append(c)
        elif c == '"':
            if not in_single: in_double = not in_double
            current_col.append(c)
        elif not in_single and not in_double:
            if c == '(': paren_depth += 1
            elif c == ')': paren_depth -= 1
            elif c == '[': bracket_depth += 1
            elif c == ']': bracket_depth -= 1
            elif c == '{': brace_depth += 1
            elif c == '}': brace_depth -= 1
            
            if paren_depth == 0 and bracket_depth == 0 and brace_depth == 0:
                if c == ',':
                    columns.append("".join(current_col))
                    current_col = []
                    i += 1
                    continue
                if (check_keyword(i, "ORDER BY") or 
                    check_keyword(i, "LIMIT") or 
                    check_keyword(i, "SKIP") or
                    check_keyword(i, "OFFSET")):
                    end_idx = i
                    break
            current_col.append(c)
        else:
            current_col.append(c)
        i += 1
        
    if current_col:
        columns.append("".join(current_col))
        
    suffix = remainder[end_idx:]
    
    alias_map = {}
    new_columns = []
    
    for idx, col in enumerate(columns):
        col_str = col.strip()
        if not col_str:
            new_columns.append(col)
            continue
            
        import re
        # Check if user explicitly aliased (AS <alias>)
        match = re.search(r'(?i)\s+AS\s+([a-zA-Z0-9_`]+)$', col_str)
        if match:
            new_columns.append(col)
        else:
            c_alias = f"c_{idx}"
            
            has_distinct = ""
            if idx == 0 and col_str.upper().startswith("DISTINCT "):
                has_distinct = col_str[:9]
                col_str = col_str[9:].lstrip()
            
            alias_map[c_alias] = col_str
            
            if has_distinct:
                new_columns.append(f"{has_distinct}{col_str} AS {c_alias}")
            else:
                new_columns.append(f"{col_str} AS {c_alias}")
                
    if suffix and not suffix[0].isspace():
        suffix = " " + suffix
    final_query = prefix + ", ".join(new_columns) + suffix
    return final_query, alias_map


class SpannerSessionWrapper:
    def __init__(self, database_obj, graph_name):
        self.database = database_obj
        self.graph_name = graph_name
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    def _format_gql(self, query, parameters):
        import re
        gql_query = query
        if not gql_query.strip().upper().startswith("GRAPH"):
            gql_query = f"GRAPH {self.graph_name}\n{gql_query}"
            
        # Spanner GQL variables are @var instead of $var
        gql_query = re.sub(r'\$(\w+)', r'@\1', gql_query)
        
        # Translate Cypher string matching operators to Spanner Google SQL equivalents
        expr_pat = r'((?:\w+\([\w\.@\'"]+\))|[\w\.@\'"]+)'
        gql_query = re.sub(rf'(?i){expr_pat}\s+CONTAINS\s+{expr_pat}', r'STRPOS(\1, \2) > 0', gql_query)
        gql_query = re.sub(rf'(?i){expr_pat}\s+STARTS\s+WITH\s+{expr_pat}', r'STARTS_WITH(\1, \2)', gql_query)
        gql_query = re.sub(rf'(?i){expr_pat}\s+ENDS\s+WITH\s+{expr_pat}', r'ENDS_WITH(\1, \2)', gql_query)
        
        # Translate Cypher functions
        gql_query = re.sub(r'(?i)\btoLower\(', 'LOWER(', gql_query)
        
        # Wrap all node and edge labels in backticks to prevent reserved word collisions
        gql_query = re.sub(r'\[([a-zA-Z0-9_]*):([a-zA-Z0-9_]+)', r'[\1:`\2`', gql_query)
        gql_query = re.sub(r'\(([a-zA-Z0-9_]*):([a-zA-Z0-9_]+)', r'(\1:`\2`', gql_query)
        
        # Guard against nested dicts leaking into GQL execution engine
        safe_parameters = {}
        for k, v in parameters.items():
            if isinstance(v, (dict, list)):
                safe_parameters[k] = str(v)
            else:
                safe_parameters[k] = v

        return gql_query, safe_parameters

    def _execute_translations(self, transaction, translations):
        import google.cloud.spanner as spanner
        from google.cloud.spanner_v1.data_types import JsonObject
        import uuid
        import json
        import re

        node_merges = []
        edge_merges = []

        for sql_op in translations:
            if isinstance(sql_op, dict):
                if sql_op.get("type") == "node_merge":
                    node_merges.append(sql_op)
                elif sql_op.get("type") == "edge_merge":
                    edge_merges.append(sql_op)

        # PASS 1: Bulk Resolution using UNNEST arrays
        lookups = {}
        lookup_mappings = {}
        for idx, op in enumerate(edge_merges):
            if "original_parameters" not in op:
                continue # Simplified native payload, skip lookups

            params = op["original_parameters"]
            match_lookups = op.get("match_lookups", {})
            src_var, dst_var = op["src_var"], op["dst_var"]
            src_pk, dst_pk = op["src_pk"], op["dst_pk"]
            
            src_val = params.get(f"{src_var}_pk", params.get(src_pk))
            dst_val = params.get(f"{dst_var}_pk", params.get(dst_pk))
            
            op["_src_val"] = src_val
            op["_dst_val"] = dst_val
            
            def add_lookup(var, pk, role):
                if var in match_lookups:
                    table = match_lookups[var]["table"]
                    pk_col = match_lookups[var]["pk"]
                    criteria = match_lookups[var]["criteria"]
                    match_col = None
                    match_val = None
                    for k, param_type, v in criteria:
                        if param_type == 'param':
                            match_col = k
                            match_val = params.get(v)
                            break
                    if match_col and match_val:
                        key = (table, pk_col, match_col)
                        lookups.setdefault(key, set()).add(match_val)
                        lookup_mappings[(idx, role)] = (key, match_val)

            if not src_val: add_lookup(src_var, src_pk, "src")
            if not dst_val: add_lookup(dst_var, dst_pk, "dst")

        resolved_ids = {}
        # Fetch all grouped lookups using UNNEST for extreme performance
        for (table, pk_col, match_col), values in lookups.items():
            if not values: continue
            vlist = list(values)
            for i in range(0, len(vlist), 5000):
                chunk = vlist[i:i+5000]
                query = f"SELECT {pk_col}, {match_col} FROM `{table}` WHERE {match_col} IN UNNEST(@vals)"
                results = transaction.execute_sql(query, params={"vals": chunk})
                for row in results:
                    resolved_ids[(table, match_col, row[1])] = row[0]
        
        # PASS 2: Mutation Construction
        mutations_by_table = {}
        
        def get_insertable_cols(table_name, is_edge=False):
            if is_edge:
                schema_str = EDGE_TABLES.get(table_name, {}).get("properties", "")
                cols = {"id", "src_id", "dst_id"}
            else:
                schema_str = NODE_TABLES.get(table_name, {}).get("schema", "")
                cols = set()
            for part in schema_str.split(','):
                part = part.strip()
                if not part or " AS " in part.upper(): continue
                cols.add(part.split()[0])
            return cols
        
        for op in node_merges:
            table = op["table"]
            params = op["_params"]
            allowed_cols = get_insertable_cols(table, False)
            
            final_params = {}
            props_json = {}
            existing_props = params.get("properties", {})
            if isinstance(existing_props, dict):
                props_json.update(existing_props)
                
            for k, v in params.items():
                if k == "properties": continue
                if k in allowed_cols:
                    final_params[k] = v
                else:
                    props_json[k] = v
                    
            if props_json and "properties" in allowed_cols:
                final_params["properties"] = props_json
                
            mutations_by_table.setdefault(table, []).append({
                "cols": tuple(final_params.keys()), 
                "vals": [JsonObject(v) if isinstance(v, dict) else v for v in final_params.values()]
            })

        for idx, op in enumerate(edge_merges):
            table = op["edge_label"]
            allowed_cols = get_insertable_cols(table, True)
            
            final_sql_params = op["sql_params"].copy()
            
            if "original_parameters" in op:
                params = op["original_parameters"]
                
                src_val = op["_src_val"]
                dst_val = op["_dst_val"]
                
                if (idx, "src") in lookup_mappings:
                    key, mval = lookup_mappings[(idx, "src")]
                    src_val = resolved_ids.get((key[0], key[2], mval)) or src_val
                    
                if (idx, "dst") in lookup_mappings:
                    key, mval = lookup_mappings[(idx, "dst")]
                    dst_val = resolved_ids.get((key[0], key[2], mval)) or dst_val
                
                final_src_val = src_val or params.get(op["src_pk"]) or f"dummy_{op['src_var']}"
                final_dst_val = dst_val or params.get(op["dst_pk"]) or f"dummy_{op['dst_var']}"
                
                edge_props_raw = op["edge_props_raw"]
                edge_id = str(uuid.uuid5(uuid.NAMESPACE_OID, f"{table}_{final_src_val}_{final_dst_val}_{edge_props_raw}"))
                
                final_sql_params["id"] = edge_id
                final_sql_params["src_id"] = final_src_val
                final_sql_params["dst_id"] = final_dst_val

            final_params = {}
            props_json = {}
            existing_props = final_sql_params.get("properties", {})
            if isinstance(existing_props, dict):
                props_json.update(existing_props)
                
            for k, v in final_sql_params.items():
                if k == "properties": continue
                if k in allowed_cols:
                    final_params[k] = v
                else:
                    props_json[k] = v
                    
            if props_json and "properties" in allowed_cols:
                final_params["properties"] = props_json
                
            cols_list = []
            vals_list = []
            for k, v in final_params.items():
                cols_list.append(k)
                vals_list.append(JsonObject(v) if isinstance(v, dict) else v)

            mutations_by_table.setdefault(table, []).append({"cols": tuple(cols_list), "vals": vals_list})
            
        # Group mutations by signature to use efficient transaction.insert_or_update
        for table, items in mutations_by_table.items():
            sigs = {}
            for item in items:
                sig = item["cols"]
                sigs.setdefault(sig, []).append(item["vals"])
            
            for cols, values_list in sigs.items():
                for i in range(0, len(values_list), 1000):
                    transaction.insert_or_update(
                        table=table,
                        columns=list(cols),
                        values=values_list[i:i+1000]
                    )

    def run_batch(self, batch_queries: list):
        """Executes a list of raw DICT payloads natively. Cypher strings are no longer supported in batching."""
        all_translations = []
        for item in batch_queries:
            if isinstance(item, tuple) and len(item) == 2:
                q, p = item
                if isinstance(q, dict):
                    all_translations.append(q)
                else:
                    warning_logger(f"Skipping legacy string Cypher query in run_batch. Please migrate to native dictionary payload: {q[:50]}")
            elif isinstance(item, dict):
                all_translations.append(item)
            
        if all_translations:
            import time
            print(f"[Spanner Batch] Executing combined transaction of {len(all_translations)} DML mutations gathered from {len(batch_queries)} inputs...", flush=True)
            def execute_all_mutations(transaction):
                self._execute_translations(transaction, all_translations)
            start_time = time.time()
            self.database.run_in_transaction(execute_all_mutations)
            end_time = time.time()
            print(f"[Spanner Batch] Transaction completed in {end_time - start_time:.2f} seconds.", flush=True)

    def run(self, query, **parameters):
        if isinstance(query, dict):
            self.run_batch([query])
            return SpannerResultWrapper([])
            
        # 0. Alias the return clause to ensure GQL compliance
        aliased_query, col_map = _auto_alias_return_clause(query)

        # 1. Handle Cascade Deletes intercept natively
        if "DETACH DELETE" in aliased_query.upper():
            return self._execute_cascade_delete(parameters)

        # Pure GQL Execution
        gql_query, safe_parameters = self._format_gql(aliased_query, parameters)
        
        try:
            with self.database.snapshot() as snapshot:
                results = snapshot.execute_sql(gql_query, params=safe_parameters)
                # Convert to list of dicts immediately so we can close snapshot
                formatted_results = []
                fields = None
                for row in results:
                    if fields is None:
                        fields = [f.name for f in results.fields]
                    
                    mapped_fields = [col_map.get(f, f) for f in fields]
                    formatted_results.append(dict(zip(mapped_fields, row)))
                return SpannerResultWrapper(formatted_results)
                    
        except Exception as e:
            err_str = str(e).lower()
            if "already exists" in err_str:
                return SpannerResultWrapper([])
            if "google.api_core.exceptions" in str(type(e)):
                # Abstract away Spanner syntax/validation errors to the original expected exception
                from neo4j.exceptions import CypherSyntaxError
                error_logger(f"Spanner GQL rejected syntax/properties: {query[:100]}... Error: {e}")
                raise CypherSyntaxError(f"Backend rejected Cypher Query: {str(e)}")
            error_logger(f"Spanner Query failed: {query[:100]}... Error: {e}")
            raise

    def _execute_cascade_delete(self, parameters):
        def execute_cascade_deletes(transaction):
            path_val = parameters.get("path")
            if not path_val: return
            
            warning_logger(f"Translating unsupported GQL DETACH DELETE into cascading Spanner SQL deletions for path: {path_val}")
            
            tables_with_path = [
                "Function", "Class", "Variable", "Parameter", 
                "Record", "Interface", "Struct", "Enum", "Union", 
                "Property", "Annotation", "Trait", "Macro",
                "File", "Directory", "Repository",
            ]
            
            for table in tables_with_path:
                try:
                    transaction.execute_update(
                        f"DELETE FROM `{table}` WHERE STARTS_WITH(path, @path)",
                        params={"path": path_val}
                    )
                except Exception:
                    pass # Table might not exist or lacks standard path column
        
        self.database.run_in_transaction(execute_cascade_deletes)
        return SpannerResultWrapper([])

class SpannerDriverWrapper:
    def __init__(self, database_obj, graph_name):
        self.database = database_obj
        self.graph_name = graph_name
        
    def session(self):
        return SpannerSessionWrapper(self.database, self.graph_name)
        
    def close(self):
        pass
