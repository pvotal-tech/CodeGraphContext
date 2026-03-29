# src/codegraphcontext/core/database_spanner.py
import os
import re
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
            ("Function", "uid STRING(MAX), name STRING(MAX), path STRING(MAX), line_number INT64, cyclomatic_complexity INT64"),
            ("Class", "uid STRING(MAX), name STRING(MAX), path STRING(MAX), line_number INT64"),
            ("Variable", "uid STRING(MAX), name STRING(MAX), path STRING(MAX)"),
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
        translated_query, translated_params, is_sql = self._translate_query(query, parameters)
        
        try:
            if is_sql:
                def execute_mutation(transaction):
                    transaction.execute_update(translated_query, params=translated_params)
                self.database.run_in_transaction(execute_mutation)
                return SpannerResultWrapper([])
            else:
                with self.database.snapshot() as snapshot:
                    results = snapshot.execute_sql(translated_query, params=translated_params)
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

    def _translate_query(self, query: str, parameters: Dict[str, Any]) -> Tuple[str, Dict[str, Any], bool]:
        """
        Translates basic openCypher to Spanner GQL or Spanner SQL (for mutability).
        Returns (translated_query, parameters, is_sql), where is_sql=True avoids GQL routing.
        """
        # Node MERGE interception
        # MERGE (var:Label {pk: $param}) SET var += $props
        # or MERGE (var:Label {pk: $param, ...})
        merge_node_match = re.search(r'MERGE\s+\((\w+):(\w+)\s*\{([^}]+)\}\)', query)
        if merge_node_match and "-[" not in query:
            node_var = merge_node_match.group(1)
            node_label = merge_node_match.group(2)
            
            # Map Cypher property SET statements
            set_match = re.search(r'SET\s+' + node_var + r'\s*\+=\s*\$(\w+)', query)
            props_dict = {}
            if set_match:
                param_name = set_match.group(1)
                props_dict = parameters.get(param_name, {})
            
            # Reconstruct parameter dict for Spanner SQL
            sql_params = {}
            
            # Handle the PK fields
            pk_fields = merge_node_match.group(3).split(',')
            for field in pk_fields:
                if ':' in field:
                    k, v_raw = field.split(':', 1)
                    k = k.strip()
                    v_raw = v_raw.strip()
                    # Determine if value is a parameter or literal
                    if v_raw.startswith('$'):
                        target_param = v_raw[1:]
                        sql_params[k] = parameters.get(target_param)
                    else:
                        # Strip quotes for literal
                        sql_params[k] = v_raw.strip('"\'')
            
            for k, v in props_dict.items():
                if isinstance(v, (dict, list)) and k not in ['args', 'decorators']:
                    continue
                sql_params[k] = v
                
            cols = list(sql_params.keys())
            vals = [f"@{c}" for c in cols]
            
            sql_query = f"INSERT OR UPDATE Node_{node_label} ({', '.join(cols)}) VALUES ({', '.join(vals)})"
            return sql_query, sql_params, True

        # Edge MERGE interception
        # MERGE (caller)-[:CALLS {line_number: $line_number, args: $args, full_call_name: $full_call_name}]->(called)
        # or MERGE (a)-[:CONTAINS]->(b)
        merge_edge_match = re.search(r'MERGE\s+\((\w+)\)-\[:(\w+)(?:\s*\{([^}]+)\})?\]->\((\w+)\)', query)
        if merge_edge_match:
            src_var = merge_edge_match.group(1)
            edge_label = merge_edge_match.group(2)
            edge_props_raw = merge_edge_match.group(3) or ""
            dst_var = merge_edge_match.group(4)
            
            def guess_node_type(var_name):
                var_mapping = {
                    'r': 'Repository', 'f': 'File', 'd': 'Directory', 'm': 'Module',
                    'c': 'Class', 'fn': 'Function', 'caller': 'Function', 'called': 'Function',
                    'final_target': 'Function', 'final_caller': 'Function', 'child': 'Class', 'parent': 'Class',
                    'p': 'Repository', 'mod': 'Module', 'outer': 'Module', 'inner': 'Module',
                    'iface': 'Interface'
                }
                return var_mapping.get(var_name, var_name.capitalize())

            src_type = guess_node_type(src_var)
            dst_type = guess_node_type(dst_var)
            
            # Map CodeGraphContext's internal edge source/dest PK mappings implicitly sent by driver
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
            
            cols = list(sql_params.keys())
            vals = [f"@{c}" for c in cols]
            table_name = f"EdgeT_{edge_label}_{src_type}_{dst_type}"
            sql_query = f"INSERT OR UPDATE {table_name} ({', '.join(cols)}) VALUES ({', '.join(vals)})"
            return sql_query, sql_params, True

        # Pure GQL fallback for reads (MATCH)
        if not query.strip().upper().startswith("GRAPH"):
            query = f"GRAPH {self.graph_name}\n{query}"
            
        # Spanner GQL variables are @var instead of $var
        query = re.sub(r'\$(\w+)', r'@\1', query)
        
        # Wrap all node and edge labels in backticks to prevent reserved word collisions (e.g., CONTAINS)
        query = re.sub(r'\[([a-zA-Z0-9_]*):([a-zA-Z0-9_]+)', r'[\1:`\2`', query)
        query = re.sub(r'\(([a-zA-Z0-9_]*):([a-zA-Z0-9_]+)', r'(\1:`\2`', query)
        
        return query, parameters, False

class SpannerDriverWrapper:
    def __init__(self, database_obj, graph_name):
        self.database = database_obj
        self.graph_name = graph_name
        
    def session(self):
        return SpannerSessionWrapper(self.database, self.graph_name)
        
    def close(self):
        pass
