# src/codegraphcontext/core/database_spanner.py
import os
import re
import json
import threading
from typing import Optional, Tuple, Dict, Any, List

from codegraphcontext.utils.debug_log import debug_log, info_logger, error_logger, warning_logger

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
                        
                        self._initialize_schema()
                        info_logger("Spanner connection established and schema verified")
                    except ImportError:
                        error_logger("Google Cloud Spanner is not installed. Run 'pip install google-cloud-spanner'")
                        raise ValueError("google-cloud-spanner missing.")
                    except Exception as e:
                        error_logger(f"Failed to initialize Spanner: {e}")
                        raise

        return SpannerDriverWrapper(self._database_obj, self._graph_name)

    def _initialize_schema(self):
        """Creates Node tables, Edge tables, and the Property Graph via DDL if they don't exist."""
        # Generic CodeGraphContext schema extraction
        node_tables = [
            ("Repository", "path STRING(MAX), name STRING(MAX), is_dependency BOOL"),
            ("File", "path STRING(MAX), name STRING(MAX), relative_path STRING(MAX), is_dependency BOOL"),
            ("Directory", "path STRING(MAX), name STRING(MAX)"),
            ("Function", "uid STRING(MAX), name STRING(MAX), path STRING(MAX), line_number INT64, end_line INT64, source STRING(MAX), docstring STRING(MAX), lang STRING(MAX), cyclomatic_complexity INT64, context STRING(MAX), context_type STRING(MAX), class_context STRING(MAX), is_dependency BOOL, decorators ARRAY<STRING(MAX)>, args ARRAY<STRING(MAX)>"),
            ("Class", "uid STRING(MAX), name STRING(MAX), path STRING(MAX), line_number INT64, end_line INT64, source STRING(MAX), docstring STRING(MAX), lang STRING(MAX), context STRING(MAX), context_type STRING(MAX), is_dependency BOOL, decorators ARRAY<STRING(MAX)>"),
            ("Interface", "uid STRING(MAX), name STRING(MAX), path STRING(MAX), line_number INT64, end_line INT64, source STRING(MAX), docstring STRING(MAX), lang STRING(MAX), is_dependency BOOL, decorators ARRAY<STRING(MAX)>"),
            ("Variable", "uid STRING(MAX), name STRING(MAX), path STRING(MAX), line_number INT64, source STRING(MAX), docstring STRING(MAX), lang STRING(MAX), value STRING(MAX), context STRING(MAX), is_dependency BOOL, type STRING(MAX), class_context STRING(MAX), decorators ARRAY<STRING(MAX)>"),
        ]
        
        rel_tables = [
            ("CONTAINS", "FROM File TO Function, FROM File TO Class, FROM Class TO Function"),
            ("CALLS", "FROM Function TO Function, FROM Function TO Class"),
            ("IMPORTS", "FROM File TO Module")
        ]
        
        info_logger("Spanner Graph backend depends on strict schema definitions.")
        info_logger("To apply this schema dynamically, explicit DDL construction is required.")

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


class SpannerSessionWrapper:
    def __init__(self, database_obj, graph_name):
        self.database = database_obj
        self.graph_name = graph_name
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    def run(self, query, **parameters):
        # 1. Translate Query
        translations, is_sql = self._translate_query(query, parameters)
        
        try:
            if is_sql:
                if translations == "SYSTEM_DELETE_CASCADE":
                    def execute_cascade_deletes(transaction):
                        path_val = parameters.get("path")
                        if not path_val: return
                        
                        warning_logger(f"Translating unsupported GQL DETACH DELETE into cascading Spanner SQL deletions for path: {path_val}")
                        
                        tables_with_path = [
                            "Node_Function", "Node_Class", "Node_Variable", "Node_Parameter", 
                            "Node_Record", "Node_Interface", "Node_Struct", "Node_Enum", "Node_Union", 
                            "Node_Property", "Node_Annotation", "Node_Trait", "Node_Macro",
                            "Node_File", "Node_Directory", "Node_Repository",
                        ]
                        
                        for table in tables_with_path:
                            try:
                                transaction.execute_update(
                                    f"DELETE FROM {table} WHERE STARTS_WITH(path, @path)",
                                    params={"path": path_val}
                                )
                            except Exception:
                                pass # Table might not exist or lacks standard path column
                    
                    self.database.run_in_transaction(execute_cascade_deletes)
                    return SpannerResultWrapper([])
                    
                def execute_mutation(transaction):
                    for sql_query, sql_params in translations:
                        transaction.execute_update(sql_query, params=sql_params)
                self.database.run_in_transaction(execute_mutation)
                return SpannerResultWrapper([])
            else:
                gql_query, gql_params = translations
                with self.database.snapshot() as snapshot:
                    results = snapshot.execute_sql(gql_query, params=gql_params)
                    # Convert to list of dicts immediately so we can close snapshot
                    formatted_results = []
                    fields = None
                    for row in results:
                        if fields is None:
                            fields = [f.name for f in results.fields]
                        formatted_results.append(dict(zip(fields, row)))
                    return SpannerResultWrapper(formatted_results)
                    
        except Exception as e:
            err_str = str(e).lower()
            if "already exists" in err_str:
                return SpannerResultWrapper([])
            error_logger(f"Spanner Query failed: {query[:100]}... Error: {e}")
            raise

    def _translate_query(self, query: str, parameters: Dict[str, Any]) -> Tuple[Any, bool]:
        """
        Translates basic openCypher to Spanner GQL or a list of Spanner SQL operations.
        Returns (translations, is_sql).
        If is_sql=True, translations is either 'SYSTEM_DELETE_CASCADE' or List[Tuple[str, dict]].
        """
        # Spanner GQL restricts DML DETACH DELETE. Send it to manual cascade.
        if "DETACH DELETE" in query.upper():
            return "SYSTEM_DELETE_CASCADE", True

        if "MERGE " in query:
            sql_ops = []
            
            # 1. Node MERGE extraction
            # e.g., MERGE (var:Label {pk: $param, ...})
            for merge_node_match in re.finditer(r'MERGE\s+\(([a-zA-Z0-9_]+):([a-zA-Z0-9_]+)\s*\{([^}]+)\}\)', query):
                node_var = merge_node_match.group(1)
                node_label = merge_node_match.group(2)
                
                sql_params = {}
                
                # Handle PK mapping
                pk_fields = merge_node_match.group(3).split(',')
                for field in pk_fields:
                    if ':' in field:
                        k, v_raw = field.split(':', 1)
                        k = k.strip()
                        v_raw = v_raw.strip()
                        if v_raw.startswith('$'):
                            sql_params[k] = parameters.get(v_raw[1:])
                        else:
                            sql_params[k] = v_raw.strip('"\'')
                
                # Associated SET statements
                # e.g., SET var += $props
                set_plus_match = re.search(r'SET\s+' + node_var + r'\s*\+=\s*\$([a-zA-Z0-9_]+)', query)
                if set_plus_match:
                    prop_name = set_plus_match.group(1)
                    props_dict = parameters.get(prop_name, {})
                    for k, v in props_dict.items():
                        if isinstance(v, (dict, list)) and k not in ['args', 'decorators']:
                            continue
                        sql_params[k] = v
                        
                # e.g., SET var.prop = $val
                for set_prop_match in re.finditer(r'SET\s+' + node_var + r'\.([a-zA-Z0-9_]+)\s*=\s*\$([a-zA-Z0-9_]+)', query):
                    prop_k = set_prop_match.group(1)
                    prop_v_param = set_prop_match.group(2)
                    if prop_v_param in parameters:
                        sql_params[prop_k] = parameters[prop_v_param]
                        
                cols = []
                vals = []
                final_sql_params = {}
                for c, v in sql_params.items():
                    if isinstance(v, dict):
                        cols.append(c)
                        vals.append(f"PARSE_JSON(@{c})")
                        final_sql_params[c] = json.dumps(v)
                    else:
                        cols.append(c)
                        vals.append(f"@{c}")
                        final_sql_params[c] = v
                        
                sql_query = f"INSERT OR UPDATE Node_{node_label} ({', '.join(cols)}) VALUES ({', '.join(vals)})"
                sql_ops.append((sql_query, final_sql_params))

            # 2. Edge MERGE extraction
            # e.g., MERGE (a)-[r:LABEL {props}]->(b) or MERGE (a)-[:LABEL]->(b)
            # Regex expects optionally a variable, then a colon, then the label.
            for merge_edge_match in re.finditer(r'MERGE\s+\(([a-zA-Z0-9_]+)\)-\[(?:([a-zA-Z0-9_]+)\s*)?:([a-zA-Z0-9_]+)(?:\s*\{([^}]+)\})?\]->\(([a-zA-Z0-9_]+)\)', query):
                src_var = merge_edge_match.group(1)
                edge_var = merge_edge_match.group(2) # might be None
                edge_label = merge_edge_match.group(3)
                edge_props_raw = merge_edge_match.group(4) or ""
                dst_var = merge_edge_match.group(5)
                
                def get_node_type_from_query(var_name):
                    m = re.search(r'\(\s*' + re.escape(var_name) + r'\s*:\s*([a-zA-Z0-9_]+)', query)
                    if m: return m.group(1)
                    var_mapping = {
                        'r': 'Repository', 'f': 'File', 'd': 'Directory', 'm': 'Module',
                        'c': 'Class', 'fn': 'Function', 'caller': 'Function', 'called': 'Function',
                        'final_target': 'Function', 'final_caller': 'Function', 'child': 'Class', 'parent': 'Class',
                        'p': 'Repository', 'mod': 'Module', 'outer': 'Module', 'inner': 'Module',
                        'iface': 'Interface'
                    }
                    return var_mapping.get(var_name, var_name.capitalize())

                src_type = get_node_type_from_query(src_var)
                dst_type = get_node_type_from_query(dst_var)
                
                def guess_pk_name(ntype):
                    if ntype in ['Repository', 'File', 'Directory']: return 'path'
                    if ntype in ['Module']: return 'name'
                    return 'uid'
                    
                src_pk = guess_pk_name(src_type)
                dst_pk = guess_pk_name(dst_type)

                sql_params = {
                    "src_id": parameters.get(f"{src_var}_pk", parameters.get(src_pk, f"dummy_{src_var}")),
                    "dst_id": parameters.get(f"{dst_var}_pk", parameters.get(dst_pk, f"dummy_{dst_var}"))
                }

                if edge_props_raw:
                    for field in edge_props_raw.split(','):
                        if ':' in field:
                            k, v_raw = field.split(':', 1)
                            k = k.strip()
                            target_param = v_raw.strip()[1:]
                            if target_param in parameters:
                                sql_params[k] = parameters[target_param]
                                
                # Also handle SET r += $props if an edge var was bound
                if edge_var:
                    set_edge_plus_match = re.search(r'SET\s+' + edge_var + r'\s*\+=\s*\$([a-zA-Z0-9_]+)', query)
                    if set_edge_plus_match:
                        prop_name = set_edge_plus_match.group(1)
                        props_dict = parameters.get(prop_name, {})
                        for k, v in props_dict.items():
                            sql_params[k] = v

                cols = []
                vals = []
                final_sql_params = {}
                for c, v in sql_params.items():
                    if isinstance(v, dict):
                        cols.append(c)
                        vals.append(f"PARSE_JSON(@{c})")
                        final_sql_params[c] = json.dumps(v)
                    else:
                        cols.append(c)
                        vals.append(f"@{c}")
                        final_sql_params[c] = v

                table_name = f"EdgeT_{edge_label}_{src_type}_{dst_type}"
                sql_query = f"INSERT OR UPDATE {table_name} ({', '.join(cols)}) VALUES ({', '.join(vals)})"
                sql_ops.append((sql_query, final_sql_params))

            if sql_ops:
                return sql_ops, True

        # Pure GQL fallback for reads (MATCH)
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

        return (gql_query, safe_parameters), False

class SpannerDriverWrapper:
    def __init__(self, database_obj, graph_name):
        self.database = database_obj
        self.graph_name = graph_name
        
    def session(self):
        return SpannerSessionWrapper(self.database, self.graph_name)
        
    def close(self):
        pass
