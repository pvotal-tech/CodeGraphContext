
# src/codegraphcontext/tools/graph_builder.py
import asyncio
import uuid
import pathspec
from pathlib import Path
from typing import Any, Coroutine, Dict, Optional, Tuple
from datetime import datetime

from ..core.database_spanner import SpannerDBManager as DatabaseManager
from ..core.jobs import JobManager, JobStatus
from ..utils.debug_log import debug_log, info_logger, error_logger, warning_logger

# New imports for tree-sitter (using tree-sitter-language-pack)
from tree_sitter import Language, Parser
from ..utils.tree_sitter_manager import get_tree_sitter_manager
from ..cli.config_manager import get_config_value
from ..utils.path_ignore import file_path_has_ignore_dir_segment
import fnmatch

def to_global_uri(local_path: Path, repo_root: Path, virtual_repo_name: str) -> str:
    """Converts a local absolute path into a globally unique URI matching repo@branch$path."""
    try:
        rel = local_path.resolve().relative_to(repo_root.resolve())
        if str(rel) == '.':
            return virtual_repo_name
        return f"{virtual_repo_name}${str(rel)}"
    except ValueError:
        return str(local_path.resolve())

 
DEFAULT_IGNORE_PATTERNS = [
    # Vendor / env dirs (gitignore-style; complements IGNORE_DIRS during indexing)
    "node_modules/",
    "venv/",
    ".venv/",
    "env/",
    ".env/",
    "dist/",
    "build/",
    "target/",
    "out/",
    ".git/",
    "__pycache__/",
    "*.png",
    "*.jpg",
    "*.jpeg",
    "*.gif",
    "*.svg",
    "*.mp4",
    "*.mp3",
    "*.zip",
    "*.tar",
    "*.gz",
]

class TreeSitterParser:
    """A generic parser wrapper for a specific language using tree-sitter."""

    def __init__(self, language_name: str):
        self.language_name = language_name
        self.ts_manager = get_tree_sitter_manager()
        
        # Get the language (cached) and create a new parser for this instance
        self.language: Language = self.ts_manager.get_language_safe(language_name)
        # In tree-sitter 0.25+, Parser takes language in constructor
        self.parser = Parser(self.language)

        self.language_specific_parser = None
        if self.language_name == 'python':
            from .languages.python import PythonTreeSitterParser
            self.language_specific_parser = PythonTreeSitterParser(self)
        elif self.language_name == 'javascript':
            from .languages.javascript import JavascriptTreeSitterParser
            self.language_specific_parser = JavascriptTreeSitterParser(self)
        elif self.language_name == 'go':
            from .languages.go import GoTreeSitterParser
            self.language_specific_parser = GoTreeSitterParser(self)
        elif self.language_name == 'typescript':
            from .languages.typescript import TypescriptTreeSitterParser
            self.language_specific_parser = TypescriptTreeSitterParser(self)
        elif self.language_name == 'cpp':
            from .languages.cpp import CppTreeSitterParser
            self.language_specific_parser = CppTreeSitterParser(self)
        elif self.language_name == 'rust':
            from .languages.rust import RustTreeSitterParser
            self.language_specific_parser = RustTreeSitterParser(self)
        elif self.language_name == 'c':
            from .languages.c import CTreeSitterParser
            self.language_specific_parser = CTreeSitterParser(self)
        elif self.language_name == 'java':
            from .languages.java import JavaTreeSitterParser
            self.language_specific_parser = JavaTreeSitterParser(self)
        elif self.language_name == 'ruby':
            from .languages.ruby import RubyTreeSitterParser
            self.language_specific_parser = RubyTreeSitterParser(self)
        elif self.language_name == 'c_sharp':
            from .languages.csharp import CSharpTreeSitterParser
            self.language_specific_parser = CSharpTreeSitterParser(self)
        elif self.language_name == 'php':
            from .languages.php import PhpTreeSitterParser
            self.language_specific_parser = PhpTreeSitterParser(self)
        elif self.language_name == 'kotlin':
            from .languages.kotlin import KotlinTreeSitterParser
            self.language_specific_parser = KotlinTreeSitterParser(self)
        elif self.language_name == 'scala':
            from .languages.scala import ScalaTreeSitterParser
            self.language_specific_parser = ScalaTreeSitterParser(self)
        elif self.language_name == 'swift':
            from .languages.swift import SwiftTreeSitterParser
            self.language_specific_parser = SwiftTreeSitterParser(self)
        elif self.language_name == 'haskell':
            from .languages.haskell import HaskellTreeSitterParser
            self.language_specific_parser = HaskellTreeSitterParser(self)
        elif self.language_name == 'dart':
            from .languages.dart import DartTreeSitterParser
            self.language_specific_parser = DartTreeSitterParser(self)
        elif self.language_name == 'perl':
            from .languages.perl import PerlTreeSitterParser
            self.language_specific_parser = PerlTreeSitterParser(self)
        elif self.language_name == 'elixir':
            from .languages.elixir import ElixirTreeSitterParser
            self.language_specific_parser = ElixirTreeSitterParser(self)



    def parse(self, path: Path, is_dependency: bool = False, **kwargs) -> Dict:
        """Dispatches parsing to the language-specific parser."""
        if self.language_specific_parser:
            return self.language_specific_parser.parse(path, is_dependency, **kwargs)
        else:
            raise NotImplementedError(f"No language-specific parser implemented for {self.language_name}")

class GraphBuilder:
    """Module for building and managing the Neo4j code graph."""

    def __init__(self, db_manager: DatabaseManager, job_manager: JobManager, loop: asyncio.AbstractEventLoop):
        self.db_manager = db_manager
        self.job_manager = job_manager
        self.loop = loop
        self.driver = self.db_manager.get_driver()
        self.parsers = {
            '.py': 'python',
            '.ipynb': 'python',
            '.js': 'javascript',
            '.jsx': 'javascript',
            '.mjs': 'javascript',
            '.cjs': 'javascript',
            '.go': 'go',
            '.ts': 'typescript',
            '.tsx': 'typescript',
            '.cpp': 'cpp',
            '.h': 'cpp',
            '.hpp': 'cpp',
            '.hh': 'cpp',
            '.rs': 'rust',
            '.c': 'c',
            # '.h': 'c', # Need to write an algo for distinguishing C vs C++ headers
            '.java': 'java',
            '.rb': 'ruby',
            '.cs': 'c_sharp',
            '.php': 'php',
            '.kt': 'kotlin',
            '.scala': 'scala',
            '.sc': 'scala',
            '.swift': 'swift',
            '.hs': 'haskell',
            '.dart': 'dart',
            '.pl': 'perl',
            '.pm': 'perl',
            '.ex': 'elixir',
            '.exs': 'elixir',
        }
        self._parsed_cache = {}
        self.create_schema()

    def get_parser(self, extension: str) -> Optional[TreeSitterParser]:
        """Gets or creates a TreeSitterParser for the given extension."""
        lang_name = self.parsers.get(extension)
        if not lang_name:
            return None
        
        if lang_name not in self._parsed_cache:
            try:
                self._parsed_cache[lang_name] = TreeSitterParser(lang_name)
            except Exception as e:
                warning_logger(f"Failed to initialize parser for {lang_name}: {e}")
                return None
        return self._parsed_cache[lang_name]

    # A general schema creation based on common features across languages
    def create_schema(self):
        """Create constraints and indexes in Neo4j."""
        # When adding a new node type with a unique key, add its constraint here.
        with self.driver.session() as session:
            try:
                session.run("CREATE CONSTRAINT repository_path IF NOT EXISTS FOR (r:Repository) REQUIRE r.path IS UNIQUE")
                session.run("CREATE CONSTRAINT path IF NOT EXISTS FOR (f:File) REQUIRE f.path IS UNIQUE")
                session.run("CREATE CONSTRAINT directory_path IF NOT EXISTS FOR (d:Directory) REQUIRE d.path IS UNIQUE")
                session.run("CREATE CONSTRAINT function_unique IF NOT EXISTS FOR (f:Function) REQUIRE (f.name, f.path, f.line_number) IS UNIQUE")
                session.run("CREATE CONSTRAINT class_unique IF NOT EXISTS FOR (c:Class) REQUIRE (c.name, c.path, c.line_number) IS UNIQUE")
                session.run("CREATE CONSTRAINT trait_unique IF NOT EXISTS FOR (t:Trait) REQUIRE (t.name, t.path, t.line_number) IS UNIQUE") # Added trait constraint
                session.run("CREATE CONSTRAINT interface_unique IF NOT EXISTS FOR (i:Interface) REQUIRE (i.name, i.path, i.line_number) IS UNIQUE")
                session.run("CREATE CONSTRAINT macro_unique IF NOT EXISTS FOR (m:Macro) REQUIRE (m.name, m.path, m.line_number) IS UNIQUE")
                session.run("CREATE CONSTRAINT variable_unique IF NOT EXISTS FOR (v:Variable) REQUIRE (v.name, v.path, v.line_number) IS UNIQUE")
                session.run("CREATE CONSTRAINT module_name IF NOT EXISTS FOR (m:Module) REQUIRE m.name IS UNIQUE")
                session.run("CREATE CONSTRAINT struct_cpp IF NOT EXISTS FOR (cstruct: Struct) REQUIRE (cstruct.name, cstruct.path, cstruct.line_number) IS UNIQUE")
                session.run("CREATE CONSTRAINT enum_cpp IF NOT EXISTS FOR (cenum: Enum) REQUIRE (cenum.name, cenum.path, cenum.line_number) IS UNIQUE")
                session.run("CREATE CONSTRAINT union_cpp IF NOT EXISTS FOR (cunion: Union) REQUIRE (cunion.name, cunion.path, cunion.line_number) IS UNIQUE")
                session.run("CREATE CONSTRAINT annotation_unique IF NOT EXISTS FOR (a:Annotation) REQUIRE (a.name, a.path, a.line_number) IS UNIQUE")
                session.run("CREATE CONSTRAINT record_unique IF NOT EXISTS FOR (r:Record) REQUIRE (r.name, r.path, r.line_number) IS UNIQUE")
                session.run("CREATE CONSTRAINT property_unique IF NOT EXISTS FOR (p:Property) REQUIRE (p.name, p.path, p.line_number) IS UNIQUE")
                
                # Indexes for language attribute
                session.run("CREATE INDEX function_lang IF NOT EXISTS FOR (f:Function) ON (f.lang)")
                session.run("CREATE INDEX class_lang IF NOT EXISTS FOR (c:Class) ON (c.lang)")
                session.run("CREATE INDEX annotation_lang IF NOT EXISTS FOR (a:Annotation) ON (a.lang)")
                
                is_falkordb = getattr(self.db_manager, 'get_backend_type', lambda: 'neo4j')() != 'neo4j'
                if is_falkordb:
                    # FalkorDB uses db.idx.fulltext.createNodeIndex per label
                    for label in ['Function', 'Class']:
                        try:
                            session.run(f"CALL db.idx.fulltext.createNodeIndex('{label}', 'name', 'source', 'docstring')")
                        except Exception:
                            pass  # Index may already exist
                else:
                    session.run("""
                        CREATE FULLTEXT INDEX code_search_index IF NOT EXISTS
                        FOR (n:Function|Class|Variable)
                        ON EACH [n.name, n.source, n.docstring]
                    """)
                
                info_logger("Database schema verified/created successfully")
            except Exception as e:
                warning_logger(f"Schema creation warning: {e}")

    # Neo4j RANGE indexes have an ~8 kB key-size limit.  Long C++ template
    # function names (e.g. from llama.cpp) can exceed this, causing
    # "Property value is too large to index" errors.  We cap string properties
    # at 4096 chars, which is comfortably under the 8 kB boundary.
    _MAX_STR_LEN = 4096

    @staticmethod
    def _sanitize_props(props: Dict) -> Dict:
        """Return a copy of *props* with all values coerced to database-safe types.

        FalkorDB and KùzuDB only accept node properties that are primitives
        (str, int, float, bool, None) or flat lists of primitives.  Complex
        values such as tuples, dicts, or lists-of-dicts that come from language
        parsers (e.g. C's ``detailed_args`` or Scala's tuple ``class_context``)
        are serialized to a JSON string so the data is preserved rather than
        being silently dropped.

        Additionally, string values are truncated to _MAX_STR_LEN characters to
        avoid Neo4j's RANGE-index 8 kB property-size limit (triggered by very
        long C++ template-mangled function names).
        """
        import json

        MAX = GraphBuilder._MAX_STR_LEN

        def _is_primitive(v):
            return isinstance(v, (str, int, float, bool)) or v is None

        def _is_flat_list(v):
            return isinstance(v, list) and all(_is_primitive(item) for item in v)

        def _coerce(v):
            if isinstance(v, str):
                # Truncate long strings to stay within Neo4j RANGE index limits
                return v[:MAX] if len(v) > MAX else v
            if _is_primitive(v):
                return v
            if _is_flat_list(v):
                # Truncate any long strings in lists too
                return [s[:MAX] if isinstance(s, str) and len(s) > MAX else s for s in v]
            # Tuples, dicts, lists-of-dicts, nested structures → JSON string
            try:
                serialized = json.dumps(v, default=str)
                return serialized[:MAX] if len(serialized) > MAX else serialized
            except Exception:
                s = str(v)
                return s[:MAX] if len(s) > MAX else s

        return {k: _coerce(v) for k, v in props.items()}


    def _pre_scan_for_imports(self, files: list[Path]) -> dict:
        """Dispatches pre-scan to the correct language-specific implementation."""
        imports_map = {}
        
        # Group files by language/extension
        files_by_lang = {}
        for file in files:
            if file.suffix in self.parsers:
                lang_ext = file.suffix
                if lang_ext not in files_by_lang:
                    files_by_lang[lang_ext] = []
                files_by_lang[lang_ext].append(file)

        if '.py' in files_by_lang:
            from .languages import python as python_lang_module
            imports_map.update(python_lang_module.pre_scan_python(files_by_lang['.py'], self.get_parser('.py')))
        if '.ipynb' in files_by_lang:
            from .languages import python as python_lang_module
            imports_map.update(python_lang_module.pre_scan_python(files_by_lang['.ipynb'], self.get_parser('.ipynb')))
        if '.js' in files_by_lang:
            from .languages import javascript as js_lang_module
            imports_map.update(js_lang_module.pre_scan_javascript(files_by_lang['.js'], self.get_parser('.js')))
        if '.jsx' in files_by_lang:
            from .languages import javascript as js_lang_module
            imports_map.update(js_lang_module.pre_scan_javascript(files_by_lang['.jsx'], self.get_parser('.jsx')))
        if '.mjs' in files_by_lang:
            from .languages import javascript as js_lang_module
            imports_map.update(js_lang_module.pre_scan_javascript(files_by_lang['.mjs'], self.get_parser('.mjs')))
        if '.cjs' in files_by_lang:
            from .languages import javascript as js_lang_module
            imports_map.update(js_lang_module.pre_scan_javascript(files_by_lang['.cjs'], self.get_parser('.cjs')))
        if '.go' in files_by_lang:
             from .languages import go as go_lang_module
             imports_map.update(go_lang_module.pre_scan_go(files_by_lang['.go'], self.get_parser('.go')))
        if '.ts' in files_by_lang:
            from .languages import typescript as ts_lang_module
            imports_map.update(ts_lang_module.pre_scan_typescript(files_by_lang['.ts'], self.get_parser('.ts')))
        if '.tsx' in files_by_lang:
            from .languages import typescriptjsx as tsx_lang_module
            imports_map.update(tsx_lang_module.pre_scan_typescript(files_by_lang['.tsx'], self.get_parser('.tsx')))
        if '.cpp' in files_by_lang:
            from .languages import cpp as cpp_lang_module
            imports_map.update(cpp_lang_module.pre_scan_cpp(files_by_lang['.cpp'], self.get_parser('.cpp')))
        if '.h' in files_by_lang:
            from .languages import cpp as cpp_lang_module
            imports_map.update(cpp_lang_module.pre_scan_cpp(files_by_lang['.h'], self.get_parser('.h')))
        if '.hpp' in files_by_lang:
            from .languages import cpp as cpp_lang_module
            imports_map.update(cpp_lang_module.pre_scan_cpp(files_by_lang['.hpp'], self.get_parser('.hpp')))
        if '.hh' in files_by_lang:
            from .languages import cpp as cpp_lang_module
            imports_map.update(cpp_lang_module.pre_scan_cpp(files_by_lang['.hh'], self.get_parser('.hh')))
        if '.rs' in files_by_lang:
            from .languages import rust as rust_lang_module
            imports_map.update(rust_lang_module.pre_scan_rust(files_by_lang['.rs'], self.get_parser('.rs')))
        if '.c' in files_by_lang:
            from .languages import c as c_lang_module
            imports_map.update(c_lang_module.pre_scan_c(files_by_lang['.c'], self.get_parser('.c')))
        elif '.java' in files_by_lang:
            from .languages import java as java_lang_module
            imports_map.update(java_lang_module.pre_scan_java(files_by_lang['.java'], self.get_parser('.java')))
        elif '.rb' in files_by_lang:
            from .languages import ruby as ruby_lang_module
            imports_map.update(ruby_lang_module.pre_scan_ruby(files_by_lang['.rb'], self.get_parser('.rb')))
        elif '.cs' in files_by_lang:
            from .languages import csharp as csharp_lang_module
            imports_map.update(csharp_lang_module.pre_scan_csharp(files_by_lang['.cs'], self.get_parser('.cs')))
        if '.kt' in files_by_lang:
            from .languages import kotlin as kotlin_lang_module
            imports_map.update(kotlin_lang_module.pre_scan_kotlin(files_by_lang['.kt'], self.get_parser('.kt')))
        if '.scala' in files_by_lang:
            from .languages import scala as scala_lang_module
            imports_map.update(scala_lang_module.pre_scan_scala(files_by_lang['.scala'], self.get_parser('.scala')))
        if '.sc' in files_by_lang:
            from .languages import scala as scala_lang_module
            imports_map.update(scala_lang_module.pre_scan_scala(files_by_lang['.sc'], self.get_parser('.sc')))
        if '.swift' in files_by_lang:
            from .languages import swift as swift_lang_module
            imports_map.update(swift_lang_module.pre_scan_swift(files_by_lang['.swift'], self.get_parser('.swift')))
        if '.dart' in files_by_lang:
            from .languages import dart as dart_lang_module
            imports_map.update(dart_lang_module.pre_scan_dart(files_by_lang['.dart'], self.get_parser('.dart')))
        if '.pl' in files_by_lang:
            from .languages import perl as perl_lang_module
            imports_map.update(perl_lang_module.pre_scan_perl(files_by_lang['.pl'], self.get_parser('.pl')))
        if '.pm' in files_by_lang:
            from .languages import perl as perl_lang_module
            imports_map.update(perl_lang_module.pre_scan_perl(files_by_lang['.pm'], self.get_parser('.pm')))
        if '.ex' in files_by_lang:
            from .languages import elixir as elixir_lang_module
            imports_map.update(elixir_lang_module.pre_scan_elixir(files_by_lang['.ex'], self.get_parser('.ex')))
        if '.exs' in files_by_lang:
            from .languages import elixir as elixir_lang_module
            imports_map.update(elixir_lang_module.pre_scan_elixir(files_by_lang['.exs'], self.get_parser('.exs')))

        return imports_map

    # Language-agnostic method
    def add_repository_to_graph(self, repo_uri: str, repo_name: str, is_dependency: bool = False):
        """Adds a repository node using its virtual URI as the unique key."""
        with self.driver.session() as session:
            payload = {
                "type": "node_merge",
                "table": "Repository",
                "_params": {"path": repo_uri, "name": repo_name, "is_dependency": is_dependency}
            }
            if hasattr(session, 'run_batch'):
                session.run_batch([payload])

    # First pass to add file and its contents
    def add_file_to_graph(self, file_data: Dict, repo_name: str, imports_map: dict, global_batch: list = None):
        calls_count = len(file_data.get('function_calls', []))
        debug_log(f"Executing add_file_to_graph for {file_data.get('path', 'unknown')} - Calls found: {calls_count}")
        """Adds a file and its contents within a single, unified session."""
        file_uri = file_data['path']
        repo_uri = file_data['repo_path']
        file_path_str = file_uri
        
        if file_uri.startswith(repo_uri + "$"):
            relative_path = file_uri[len(repo_uri)+1:]
        else: # Safety fallback
            relative_path = Path(file_uri).name
            
        file_name = Path(relative_path).name
        is_dependency = file_data.get('is_dependency', False)

        import contextlib
        @contextlib.contextmanager
        def get_session():
            if global_batch is not None:
                yield None
            else:
                with self.driver.session() as s:
                    yield s

        with get_session() as session:
            batch_support = True
            batch_queries = global_batch if global_batch is not None else []

            def execute_or_queue(payload: dict):
                batch_queries.append(payload)

            execute_or_queue({
                "type": "node_merge",
                "table": "File",
                "_params": {
                    "path": file_uri, 
                    "name": file_name, 
                    "relative_path": relative_path, 
                    "is_dependency": is_dependency
                }
            })

            parent_path = repo_uri
            parent_label = 'Repository'
            
            parts = Path(relative_path).parts[:-1]

            for part in parts:
                if parent_label == 'Repository':
                    current_path_str = f"{parent_path}${part}"
                else:
                    current_path_str = f"{parent_path}/{part}"
                
                execute_or_queue({
                    "type": "node_merge",
                    "table": "Directory",
                    "_params": {"path": current_path_str, "name": part}
                })
                
                import uuid
                edge_id = str(uuid.uuid5(uuid.NAMESPACE_OID, f"CONTAINS_{parent_path}_{current_path_str}_"))
                execute_or_queue({
                    "type": "edge_merge",
                    "edge_label": "CONTAINS",
                    "sql_params": {
                        "id": edge_id,
                        "src_id": parent_path,
                        "dst_id": current_path_str
                    }
                })

                parent_path = current_path_str
                parent_label = 'Directory'

            import uuid
            file_edge_id = str(uuid.uuid5(uuid.NAMESPACE_OID, f"CONTAINS_{parent_path}_{file_uri}_"))
            execute_or_queue({
                "type": "edge_merge",
                "edge_label": "CONTAINS",
                "sql_params": {
                    "id": file_edge_id,
                    "src_id": parent_path,
                    "dst_id": file_uri
                }
            })

            # CONTAINS relationships for functions, classes, and variables
            # To add a new language-specific node type (e.g., 'Trait' for Rust):
            # 1. Ensure your language-specific parser returns a list under a unique key (e.g., 'traits': [...] ).
            # 2. Add a new constraint for the new label in the `create_schema` method.
            # 3. Add a new entry to the `item_mappings` list below (e.g., (file_data.get('traits', []), 'Trait') ).
            item_mappings = [
                (file_data.get('functions', []), 'Function'),
                (file_data.get('classes', []), 'Class'),
                (file_data.get('traits', []), 'Trait'), # <-- Added trait mapping
                (file_data.get('variables', []), 'Variable'),
                (file_data.get('interfaces', []), 'Interface'),
                (file_data.get('macros', []), 'Macro'),
                (file_data.get('structs',[]), 'Struct'),
                (file_data.get('enums',[]), 'Enum'),
                (file_data.get('unions',[]), 'Union'),
                (file_data.get('records',[]), 'Record'),
                (file_data.get('properties',[]), 'Property'),
            ]
            for item_data, label in item_mappings:
                for item in item_data:
                    # Ensure cyclomatic_complexity is set for functions
                    if label == 'Function' and 'cyclomatic_complexity' not in item:
                        item['cyclomatic_complexity'] = 1 # Default value

                    # Pre-compute UUID deterministic UID just like backend
                    import uuid
                    uid_str = f"{label}_{file_path_str}_{item['name']}_{item.get('line_number', '')}_{item.get('source', '')}"
                    item_uid = str(uuid.uuid5(uuid.NAMESPACE_OID, uid_str))
                    item['uid'] = item_uid # Make it available for AST caching later

                    safe_props = self._sanitize_props(item)
                    safe_props["name"] = item['name']
                    safe_props["path"] = file_path_str
                    safe_props["line_number"] = item.get('line_number')
                    safe_props["uid"] = item_uid

                    try:
                        execute_or_queue({
                            "type": "node_merge",
                            "table": label,
                            "_params": safe_props
                        })
                        
                        f_edge_id = str(uuid.uuid5(uuid.NAMESPACE_OID, f"CONTAINS_{file_path_str}_{item_uid}_"))
                        execute_or_queue({
                            "type": "edge_merge",
                            "edge_label": "CONTAINS",
                            "sql_params": {
                                "id": f_edge_id,
                                "src_id": file_path_str,
                                "dst_id": item_uid
                            }
                        })
                    except Exception as node_err:
                        err_str = str(node_err)
                        if "too large to index" in err_str or "property size" in err_str.lower():
                            warning_logger(
                                f"Skipping {label} '{item['name']}' in {file_path_str}: "
                                f"property value too large for index (name length={len(item['name'])})"
                            )
                        else:
                            raise  # Re-raise unexpected errors
                    
                    if label == 'Function':
                        for arg_name in item.get('args', []):
                            p_uid_str = f"Parameter_{file_path_str}_{arg_name}_{item.get('line_number', '')}_"
                            p_uid = str(uuid.uuid5(uuid.NAMESPACE_OID, p_uid_str))
                            
                            execute_or_queue({
                                "type": "node_merge",
                                "table": "Parameter",
                                "_params": {
                                    "uid": p_uid,
                                    "name": arg_name,
                                    "path": file_path_str,
                                    "function_line_number": item.get('line_number')
                                }
                            })
                            
                            p_edge_id = str(uuid.uuid5(uuid.NAMESPACE_OID, f"HAS_PARAMETER_{item_uid}_{p_uid}_"))
                            execute_or_queue({
                                "type": "edge_merge",
                                "edge_label": "HAS_PARAMETER",
                                "sql_params": {
                                    "id": p_edge_id,
                                    "src_id": item_uid,
                                    "dst_id": p_uid
                                }
                            })

            # --- NEW: persist Ruby Modules ---
            for m in file_data.get('modules', []):
                execute_or_queue({
                    "type": "node_merge",
                    "table": "Module",
                    "_params": {"name": m["name"], "lang": file_data.get("lang")}
                })

            # Create CONTAINS relationships for nested functions
            for item in file_data.get('functions', []):
                if item.get("context_type") == "function_definition":
                    outer_uid = None
                    for pot in file_data.get('functions', []):
                        if pot['name'] == item['context']:
                            outer_uid = pot.get('uid')
                            break
                    if outer_uid:
                        execute_or_queue({
                            "type": "edge_merge",
                            "edge_label": "CONTAINS",
                            "sql_params": {
                                "id": str(uuid.uuid5(uuid.NAMESPACE_OID, f"CONTAINS_{outer_uid}_{item['uid']}_")),
                                "src_id": outer_uid,
                                "dst_id": item['uid']
                            }
                        })

            # Handle imports and create IMPORTS relationships
            for imp in file_data.get('imports', []):
                info_logger(f"Processing import: {imp}")
                lang = file_data.get('lang')
                module_name = imp.get('source') if lang == 'javascript' else imp.get('name')
                if not module_name: continue
                
                # Use a map for relationship properties to handle optional alias and line_number
                rel_props = {}
                if lang == 'javascript':
                    rel_props['imported_name'] = imp.get('name', '*')
                else:
                    if 'full_import_name' in imp:
                        rel_props['full_import_name'] = imp.get('full_import_name')
                        
                if imp.get('alias'):
                    rel_props['alias'] = imp.get('alias')
                if imp.get('line_number'):
                    rel_props['line_number'] = imp.get('line_number')

                if lang != 'javascript':
                    sanitized = self._sanitize_props(imp)
                    for k, v in sanitized.items():
                        if k not in ['name', 'path', 'line_number', 'alias', 'full_import_name', 'source']:
                            rel_props[k] = v

                node_payload = {"name": module_name}
                if lang != 'javascript' and 'full_import_name' in imp:
                    node_payload["full_import_name"] = imp.get('full_import_name')
                    
                execute_or_queue({
                    "type": "node_merge",
                    "table": "Module",
                    "_params": node_payload
                })

                import uuid
                r_edge_id = str(uuid.uuid5(uuid.NAMESPACE_OID, f"IMPORTS_{file_path_str}_{module_name}_"))
                rel_params = {
                    "id": r_edge_id,
                    "src_id": file_path_str,
                    "dst_id": module_name
                }
                for k, v in rel_props.items():
                    rel_params[k] = v

                execute_or_queue({
                    "type": "edge_merge",
                    "edge_label": "IMPORTS",
                    "sql_params": rel_params
                })

            # Handle CONTAINS relationship between class to their children like variables
            local_class_names = {c['name']: c.get('uid') for c in file_data.get('classes', [])}
            for func in file_data.get('functions', []):
                class_context = func.get('class_context')
                if class_context:
                    # Try same-file match first (Python, JS, etc.)
                    c_uid = local_class_names.get(class_context)
                    if c_uid:
                        execute_or_queue({
                            "type": "edge_merge",
                            "edge_label": "CONTAINS",
                            "sql_params": {
                                "id": str(uuid.uuid5(uuid.NAMESPACE_OID, f"CONTAINS_{c_uid}_{func['uid']}_")),
                                "src_id": c_uid,
                                "dst_id": func['uid']
                            }
                        })
                    else:
                        # Cross-file match for C/C++ where class is in .h and method in .cpp.
                        execute_or_queue({
                            "type": "edge_merge",
                            "edge_label": "CONTAINS",
                            "sql_params": {
                                "dst_id": func['uid']
                            },
                            "original_parameters": {
                                "class_name": class_context
                            },
                            "match_lookups": {
                                "c": {
                                    "table": "Class",
                                    "pk": "uid",
                                    "criteria": [["name", "param", "class_name"]]
                                }
                            },
                            "src_var": "c",
                            "dst_var": "fn",
                            "src_pk": "uid",
                            "dst_pk": "uid",
                            "edge_props_raw": ""
                        })

            # --- NEW: Class INCLUDES Module (Ruby mixins) ---
            for inc in file_data.get('module_inclusions', []):
                execute_or_queue({
                    "type": "node_merge",
                    "table": "Module",
                    "_params": {"name": inc["module"]}
                })
                
                execute_or_queue({
                    "type": "edge_merge",
                    "edge_label": "INCLUDES",
                    "sql_params": {},
                    "original_parameters": {
                        "class_name": inc["class"],
                        "path": file_path_str,
                        "module_name": inc["module"]
                    },
                    "match_lookups": {
                        "c": {
                            "table": "Class",
                            "pk": "uid",
                            "criteria": [["name", "param", "class_name"], ["path", "param", "path"]]
                        },
                        "m": {
                            "table": "Module",
                            "pk": "name",
                            "criteria": [["name", "param", "module_name"]]
                        }
                    },
                    "src_var": "c",
                    "dst_var": "m",
                    "src_pk": "uid",
                    "dst_pk": "name",
                    "edge_props_raw": ""
                })

            if global_batch is None and session and batch_queries:
                session.run_batch(batch_queries)
                batch_queries.clear()

            # Class inheritance is handled in a separate pass after all files are processed.
            # Function calls are also handled in a separate pass after all files are processed.

    # Second pass to create relationships that depend on all files being present like call functions and class inheritance
    def _populate_resolution_cache(self, all_file_data: list[Dict]):
        """Build precise in-memory sets of functions and classes that exist in the current indexing batch.
        This provides O(1) resolution validation locally without sequential network queries.
        """
        import time
        start_time = time.time()
        
        self.exact_functions = {}
        self.exact_classes = {}
        self._exact_class_has_init = {}
        
        for fd in all_file_data:
            path = fd.get('path')
            if not path:
                continue
                
            for fn in fd.get('functions', []):
                uid = fn.get('uid')
                self.exact_functions[(fn['name'], path)] = uid
                # Register constructors
                if fn.get('class_context') and fn.get('name') in ["__init__", "constructor"]:
                    self._exact_class_has_init[(fn['class_context'], path)] = uid
                    
            for cls in fd.get('classes', []):
                uid = cls.get('uid')
                self.exact_classes[(cls['name'], path)] = uid

        debug_log(f"Resolution cache built internally in {time.time()-start_time:.2f}s: {len(self.exact_functions)} Functions/Methods, {len(self.exact_classes)} Classes.")

    def _safe_run_create(self, session, query, params) -> bool:
        """Helper to run a creation query safely, catching exceptions and checking result."""
        try:
            result = session.run(query, **params)
            row = result.single()
            return row is not None and row.get('created', 0) > 0
        except Exception as e:
            # Optionally log, but suppress to allow fallback
            return False

    def _create_function_calls(self, session, file_data: Dict, imports_map: dict, batch_queries: list):
        """Create CALLS relationships with a unified, prioritized logic flow for all call types."""
        caller_file_path = file_data['path']
        num_calls = len(file_data.get('function_calls', []))
        if num_calls > 0:
            debug_log(f"Creating function calls for {caller_file_path} (Count: {num_calls})")
        
        local_names = {f['name'] for f in file_data.get('functions', [])} | \
                      {c['name'] for c in file_data.get('classes', [])}
        local_imports = {imp.get('alias') or imp['name'].split('.')[-1]: imp['name'] 
                        for imp in file_data.get('imports', [])}
        
        # Check if we should skip external resolution attempts - 
        skip_external = (get_config_value("SKIP_EXTERNAL_RESOLUTION") or "false").lower() == "true"
        
        for call in file_data.get('function_calls', []):
            called_name = call['name']
            caller_context = call.get('caller_context')
            # debug_log(f"Processing call: {called_name}")
            if called_name in __builtins__: continue

            resolved_path = None
            full_call = call.get('full_name', called_name)
            base_obj = full_call.split('.')[0] if '.' in full_call else None
            
            # For chained calls like self.graph_builder.method(), we need to look up 'method'
            # For direct calls like self.method(), we can use the caller's file
            is_chained_call = full_call.count('.') > 1 if '.' in full_call else False
            
            # Determine the lookup name:
            # - For chained calls (self.attr.method), use the actual method name
            # - For direct calls (self.method or module.function), use the base object
            if is_chained_call and base_obj in ('self', 'this', 'super', 'super()', 'cls', '@'):
                lookup_name = called_name  # Use the actual method name for lookup
            else:
                lookup_name = base_obj if base_obj else called_name

            # 1. Check for local context keywords/direct local names
            # Only resolve to caller_file_path for DIRECT self/this calls, not chained ones
            if base_obj in ('self', 'this', 'super', 'super()', 'cls', '@') and not is_chained_call:
                resolved_path = caller_file_path
            elif lookup_name in local_names:
                resolved_path = caller_file_path
            
            # 2. Check inferred type if available
            elif call.get('inferred_obj_type'):
                obj_type = call['inferred_obj_type']
                possible_paths = imports_map.get(obj_type, [])
                if len(possible_paths) > 0:
                    resolved_path = possible_paths[0]
            
            # 3. Check imports map with validation against local imports
            if not resolved_path:
                possible_paths = imports_map.get(lookup_name, [])
                if len(possible_paths) == 1:
                    resolved_path = possible_paths[0]
                elif len(possible_paths) > 1:
                    if lookup_name in local_imports:
                        full_import_name = local_imports[lookup_name]
                        
                        # Optimization: Check if the FQN is directly in imports_map (from pre-scan)
                        if full_import_name in imports_map:
                             direct_paths = imports_map[full_import_name]
                             if direct_paths and len(direct_paths) == 1:
                                 resolved_path = direct_paths[0]
                        
                        if not resolved_path:
                            for path in possible_paths:
                                if full_import_name.replace('.', '/') in path:
                                    resolved_path = path
                                    break
            
            if not resolved_path:
                # Only log warning if we're not skipping external resolution
                if not skip_external:
                    warning_logger(f"Could not resolve call {called_name} (lookup: {lookup_name}) in {caller_file_path}")
                # Track that this was an unresolved external call
                is_unresolved_external = True
            else:
                is_unresolved_external = False
            # else:
            #      info_logger(f"Resolved call {called_name} -> {resolved_path}")
            
            # Legacy fallback block (was mis-indented)
            if not resolved_path:
                possible_paths = imports_map.get(lookup_name, [])
                if len(possible_paths) > 0:
                     # Final fallback: global candidate
                     # Check if it was imported explicitly, otherwise risky
                     if lookup_name in local_imports:
                         # We already tried specific matching above, but if we are here
                         # it means we had ambiguity without matching path?
                         pass
                     else:
                        # Fallback to first available if not imported? Or skip?
                        # Original logic: resolved_path = possible_paths[0]
                        # But wait, original code logic was:
                        pass
            if not resolved_path:
                if called_name in local_names:
                    resolved_path = caller_file_path
                    is_unresolved_external = False  # This is a local call, not external
                elif called_name in imports_map and imports_map[called_name]:
                    # Check if any path in imports_map for called_name matches current file's imports
                    candidates = imports_map[called_name]
                    for path in candidates:
                        for imp_name in local_imports.values():
                            if imp_name.replace('.', '/') in path:
                                resolved_path = path
                                is_unresolved_external = False  # Found a match
                                break
                        if resolved_path: break
                    if not resolved_path:
                        resolved_path = candidates[0]
                else:
                    resolved_path = caller_file_path
            
            # Skip creating CALLS relationship for unresolved external calls when skip_external is enabled
            if skip_external and is_unresolved_external:
                continue

            if caller_context and len(caller_context) == 3 and caller_context[0] is not None:
                caller_name, _, caller_line_number = caller_context
                
                # Determine caller type organically from AST
                caller_is_func = (caller_name, caller_file_path) in self.exact_functions
                caller_is_class = (caller_name, caller_file_path) in self.exact_classes
                caller_label = "Class" if caller_is_class and not caller_is_func else "Function"
                
                call_params = self._sanitize_props({
                    'caller_name': caller_name,
                    'caller_file_path': caller_file_path,
                    'caller_line_number': caller_line_number,
                    'called_name': called_name,
                    'called_file_path': resolved_path,
                    'line_number': call['line_number'],
                    'args': call.get('args', []),
                    'full_call_name': call.get('full_name', called_name)
                })
                
                # Optimistic PK injection to bypass Spanner execute_sql lookups
                caller_pk = self.exact_functions.get((caller_name, caller_file_path)) or self.exact_classes.get((caller_name, caller_file_path))

                import uuid
                edge_id = str(uuid.uuid5(uuid.NAMESPACE_OID, f"CALLS_{caller_name}_{caller_file_path}_{called_name}_{resolved_path}_{call.get('line_number', 0)}"))
                
                base_payload = {
                    "type": "edge_merge",
                    "edge_label": "CALLS",
                    "sql_params": {
                        "id": edge_id,
                        "line_number": call['line_number'],
                        "args": call.get('args', []),
                        "full_call_name": call.get('full_name', called_name)
                    },
                    "original_parameters": call_params,
                    "src_var": "caller",
                    "dst_var": "called",
                    "src_pk": "uid",
                    "dst_pk": "uid",
                    "edge_props_raw": "line_number: @line_number, args: @args, full_call_name: @full_call_name"
                }

                if caller_pk:
                    base_payload["sql_params"]["src_id"] = caller_pk
                else:
                    base_payload.setdefault("match_lookups", {})["caller"] = {
                        "table": caller_label,
                        "pk": "uid",
                        "criteria": [["name", "param", "caller_name"], ["path", "param", "caller_file_path"]]
                    }

                if resolved_path:
                    if (called_name, resolved_path) in self.exact_functions:
                        base_payload["sql_params"]["dst_id"] = self.exact_functions[(called_name, resolved_path)]
                        batch_queries.append(base_payload)
                    elif (called_name, resolved_path) in self.exact_classes:
                        called_class_pk = self.exact_classes[(called_name, resolved_path)]
                        if (called_name, resolved_path) in self._exact_class_has_init:
                            init_pk = self._exact_class_has_init[(called_name, resolved_path)]
                            base_payload["sql_params"]["dst_id"] = init_pk
                            batch_queries.append(base_payload)
                        else:
                            base_payload["sql_params"]["dst_id"] = called_class_pk
                            batch_queries.append(base_payload)
                    else:
                        base_payload.setdefault("match_lookups", {})["called"] = {
                            "table": "Function",
                            "pk": "uid",
                            "criteria": [["name", "param", "called_name"]]
                        }
                        batch_queries.append(base_payload)
                else:
                    base_payload.setdefault("match_lookups", {})["called"] = {
                        "table": "Function",
                        "pk": "uid",
                        "criteria": [["name", "param", "called_name"]]
                    }
                    batch_queries.append(base_payload)

            else:
                # File-level calls: Try Function first, then Class
                call_params = self._sanitize_props({
                    'caller_file_path': caller_file_path,
                    'called_name': called_name,
                    'called_file_path': resolved_path,
                    'line_number': call['line_number'],
                    'args': call.get('args', []),
                    'full_call_name': call.get('full_name', called_name)
                })
                
                import uuid
                edge_id = str(uuid.uuid5(uuid.NAMESPACE_OID, f"CALLS_{caller_file_path}_{called_name}_{resolved_path}_{call.get('line_number', 0)}"))
                
                base_payload = {
                    "type": "edge_merge",
                    "edge_label": "CALLS",
                    "sql_params": {
                        "id": edge_id,
                        "src_id": caller_file_path,
                        "line_number": call['line_number'],
                        "args": call.get('args', []),
                        "full_call_name": call.get('full_name', called_name)
                    },
                    "original_parameters": call_params,
                    "src_var": "caller",
                    "dst_var": "called",
                    "src_pk": "path",
                    "dst_pk": "uid",
                    "edge_props_raw": "line_number: @line_number, args: @args, full_call_name: @full_call_name"
                }

                if resolved_path:
                    if (called_name, resolved_path) in self.exact_functions:
                        base_payload["sql_params"]["dst_id"] = self.exact_functions[(called_name, resolved_path)]
                        batch_queries.append(base_payload)
                    elif (called_name, resolved_path) in self.exact_classes:
                        called_class_pk = self.exact_classes[(called_name, resolved_path)]
                        if (called_name, resolved_path) in self._exact_class_has_init:
                            init_pk = self._exact_class_has_init[(called_name, resolved_path)]
                            base_payload["sql_params"]["dst_id"] = init_pk
                            batch_queries.append(base_payload)
                        else:
                            base_payload["sql_params"]["dst_id"] = called_class_pk
                            batch_queries.append(base_payload)
                    else:
                        base_payload.setdefault("match_lookups", {})["called"] = {
                            "table": "Function",
                            "pk": "uid",
                            "criteria": [["name", "param", "called_name"]]
                        }
                        batch_queries.append(base_payload)
                else:
                    base_payload.setdefault("match_lookups", {})["called"] = {
                        "table": "Function",
                        "pk": "uid",
                        "criteria": [["name", "param", "called_name"]]
                    }
                    batch_queries.append(base_payload)


    def _create_all_function_calls(self, all_file_data: list[Dict], imports_map: dict):
        """Create CALLS relationships for all functions after all files have been processed."""
        import time
        start_cache_prep = time.time()
        debug_log(f"_create_all_function_calls called with {len(all_file_data)} files")
        
        with self.driver.session() as session:
            batch_support = hasattr(session, 'run_batch')
            batch_queries = []
            
            queue_start_time = time.time()
            for idx, file_data in enumerate(all_file_data):
                # debug_log(f"Processing function calls for file {idx+1}/{len(all_file_data)}: {file_data.get('path', 'unknown')}")
                self._create_function_calls(session, file_data, imports_map, batch_queries)
                
                # Flush batch periodically
                if len(batch_queries) > 2000:
                    print(f"[Python Processing] Prepared 2000 queries in {time.time() - queue_start_time:.2f} seconds. Executing flush...", flush=True)
                    if batch_support:
                        session.run_batch(batch_queries)
                    else:
                        for query, params in batch_queries:
                            session.run(query, params)
                    batch_queries.clear()
                    queue_start_time = time.time()
            
            # Final flush
            if batch_queries:
                print(f"[Python Processing] Prepared {len(batch_queries)} remaining queries in {time.time() - queue_start_time:.2f} seconds. Executing flush...", flush=True)
                if batch_support:
                    session.run_batch(batch_queries)
                else:
                    for query, params in batch_queries:
                        session.run(query, params)
                batch_queries.clear()

    def _create_inheritance_links(self, session, file_data: Dict, imports_map: dict, batch_queries: list):
        """Create INHERITS relationships with a more robust resolution logic."""
        caller_file_path = file_data['path']
        local_class_names = {c['name'] for c in file_data.get('classes', [])}
        # Create a map of local import aliases/names to full import names
        local_imports = {imp.get('alias') or imp['name'].split('.')[-1]: imp['name']
                         for imp in file_data.get('imports', [])}

        for class_item in file_data.get('classes', []):
            if not class_item.get('bases'):
                continue

            for base_class_str in class_item['bases']:
                if base_class_str == 'object':
                    continue

                resolved_path = None
                target_class_name = base_class_str.split('.')[-1]

                # Handle qualified names like module.Class or alias.Class
                if '.' in base_class_str:
                    lookup_name = base_class_str.split('.')[0]
                    
                    # Case 1: The prefix is a known import
                    if lookup_name in local_imports:
                        full_import_name = local_imports[lookup_name]
                        possible_paths = imports_map.get(target_class_name, [])
                        # Find the path that corresponds to the imported module
                        for path in possible_paths:
                            if full_import_name.replace('.', '/') in path:
                                resolved_path = path
                                break
                # Handle simple names
                else:
                    lookup_name = base_class_str
                    # Case 2: The base class is in the same file
                    if lookup_name in local_class_names:
                        resolved_path = caller_file_path
                    # Case 3: The base class was imported directly (e.g., from module import Parent)
                    elif lookup_name in local_imports:
                        full_import_name = local_imports[lookup_name]
                        possible_paths = imports_map.get(target_class_name, [])
                        for path in possible_paths:
                            if full_import_name.replace('.', '/') in path:
                                resolved_path = path
                                break
                    # Case 4: Fallback to global map (less reliable)
                    elif lookup_name in imports_map:
                        possible_paths = imports_map[lookup_name]
                        if len(possible_paths) == 1:
                            resolved_path = possible_paths[0]
                
                # If a path was found, create the relationship
                if resolved_path:
                    params = {
                        'child_name': class_item['name'],
                        'path': caller_file_path,
                        'parent_name': target_class_name,
                        'resolved_parent_file_path': resolved_path
                    }
                    # Optimistic PK injection
                    child_pk = self.exact_classes.get((class_item['name'], caller_file_path))
                    parent_pk = self.exact_classes.get((target_class_name, resolved_path))
                    
                    import uuid
                    edge_id = str(uuid.uuid5(uuid.NAMESPACE_OID, f"INHERITS_{class_item['name']}_{caller_file_path}_{target_class_name}_{resolved_path}"))

                    base_payload = {
                        "type": "edge_merge",
                        "edge_label": "INHERITS",
                        "sql_params": {
                            "id": edge_id
                        },
                        "original_parameters": params,
                        "src_var": "child",
                        "dst_var": "parent",
                        "src_pk": "uid",
                        "dst_pk": "uid",
                        "edge_props_raw": ""
                    }

                    if child_pk:
                        base_payload["sql_params"]["src_id"] = child_pk
                    else:
                        base_payload.setdefault("match_lookups", {})["child"] = {
                            "table": "Class",
                            "pk": "uid",
                            "criteria": [["name", "param", "child_name"], ["path", "param", "path"]]
                        }

                    if parent_pk:
                        base_payload["sql_params"]["dst_id"] = parent_pk
                    else:
                        base_payload.setdefault("match_lookups", {})["parent"] = {
                            "table": "Class",
                            "pk": "uid",
                            "criteria": [["name", "param", "parent_name"], ["path", "param", "resolved_parent_file_path"]]
                        }

                    batch_queries.append(base_payload)

    def _create_csharp_inheritance_and_interfaces(self, session, file_data: Dict, imports_map: dict, batch_queries: list):
        """Create INHERITS and IMPLEMENTS relationships for C# types."""
        if file_data.get('lang') != 'c_sharp':
            return
            
        caller_file_path = file_data['path']
        
        # Collect all local type names
        local_type_names = set()
        for type_list in ['classes', 'interfaces', 'structs', 'records']:
            local_type_names.update(t['name'] for t in file_data.get(type_list, []))
        
        # Process all type declarations that can have bases
        for type_list_name, type_label in [('classes', 'Class'), ('structs', 'Struct'), ('records', 'Record'), ('interfaces', 'Interface')]:
            for type_item in file_data.get(type_list_name, []):
                if not type_item.get('bases'):
                    continue
                
                for base_str in type_item['bases']:
                    # Clean up the base name (remove generic parameters, etc.)
                    base_name = base_str.split('<')[0].strip()
                    
                    # Determine if this is an interface
                    is_interface = False
                    resolved_path = caller_file_path
                    
                    # Check if base is a local interface
                    for iface in file_data.get('interfaces', []):
                        if iface['name'] == base_name:
                            is_interface = True
                            break
                    
                    # Check if base is in imports_map
                    if base_name in imports_map:
                        possible_paths = imports_map[base_name]
                        if len(possible_paths) > 0:
                            resolved_path = possible_paths[0]
                    
                    # For C#, first base is usually the class (if any), rest are interfaces
                    base_index = type_item['bases'].index(base_str)
                    
                    # Try to determine if it's an interface
                    if is_interface or (base_index > 0 and type_label == 'Class'):
                        # This is an IMPLEMENTS relationship
                        import uuid
                        edge_id = str(uuid.uuid5(uuid.NAMESPACE_OID, f"IMPLEMENTS_{type_item['name']}_{caller_file_path}_{base_name}"))
                        batch_queries.append({
                            "type": "edge_merge",
                            "edge_label": "IMPLEMENTS",
                            "sql_params": {"id": edge_id},
                            "original_parameters": {
                                'child_name': type_item['name'],
                                'path': caller_file_path,
                                'interface_name': base_name
                            },
                            "match_lookups": {
                                "child": {
                                    "table": type_label,
                                    "pk": "uid",
                                    "criteria": [["name", "param", "child_name"], ["path", "param", "path"]]
                                },
                                "iface": {
                                    "table": "Interface",
                                    "pk": "uid",
                                    "criteria": [["name", "param", "interface_name"]]
                                }
                            },
                            "src_var": "child",
                            "dst_var": "iface",
                            "src_pk": "uid",
                            "dst_pk": "uid",
                            "edge_props_raw": ""
                        })
                    else:
                        # This is an INHERITS relationship
                        import uuid
                        edge_id = str(uuid.uuid5(uuid.NAMESPACE_OID, f"INHERITS_{type_item['name']}_{caller_file_path}_{base_name}"))
                        batch_queries.append({
                            "type": "edge_merge",
                            "edge_label": "INHERITS",
                            "sql_params": {"id": edge_id},
                            "original_parameters": {
                                'child_name': type_item['name'],
                                'path': caller_file_path,
                                'parent_name': base_name
                            },
                            "match_lookups": {
                                "child": {
                                    "table": type_label,
                                    "pk": "uid",
                                    "criteria": [["name", "param", "child_name"], ["path", "param", "path"]]
                                },
                                "parent": {
                                    "table": "Class", # Defaulting to Class for parent
                                    "pk": "uid",
                                    "criteria": [["name", "param", "parent_name"]]
                                }
                            },
                            "src_var": "child",
                            "dst_var": "parent",
                            "src_pk": "uid",
                            "dst_pk": "uid",
                            "edge_props_raw": ""
                        })
    def _create_all_inheritance_links(self, all_file_data: list[Dict], imports_map: dict):
        """Create INHERITS relationships for all classes after all files have been processed."""
        with self.driver.session() as session:
            batch_support = hasattr(session, 'run_batch')
            batch_queries = []
            
            for file_data in all_file_data:
                # Handle C# separately
                if file_data.get('lang') == 'c_sharp':
                    self._create_csharp_inheritance_and_interfaces(session, file_data, imports_map, batch_queries)
                else:
                    self._create_inheritance_links(session, file_data, imports_map, batch_queries)
                
                # Periodically flush the batch to prevent memory blooming or hitting API limits
                if len(batch_queries) >= 2000:
                    session.run_batch(batch_queries)
                    batch_queries.clear()
                    
            if batch_queries:
                session.run_batch(batch_queries)
                batch_queries.clear()
                
    def delete_file_from_graph(self, file_uri: str):
        """Deletes a file and all its contained elements and relationships."""
        file_path_str = file_uri
        with self.driver.session() as session:
            # Fallback for Spanner and backends that do not support variable-length paths
            parent_paths = []
            if "$" in file_path_str:
                repo_uri, relative_path = file_path_str.split("$", 1)
                parts = __import__('pathlib').Path(relative_path).parts[:-1]
                current_parent = repo_uri
                parent_label = 'Repository'
                for part in parts:
                    if parent_label == 'Repository':
                        current_path_str = f"{current_parent}${part}"
                    else:
                        current_path_str = f"{current_parent}/{part}"
                    parent_paths.insert(0, current_path_str)
                    current_parent = current_path_str
                    parent_label = 'Directory'

            session.run(
                """
                MATCH (f:File {path: @path})
                OPTIONAL MATCH (f)-[:`CONTAINS`]->(element)
                DETACH DELETE f, element
                """,
                path=file_path_str,
            )
            info_logger(f"Deleted file and its elements from graph: {file_path_str}")

            for path in parent_paths:
                session.run("""
                    MATCH (d:Directory {path: @path})
                    WHERE NOT (d)-[:`CONTAINS`]->()
                    DETACH DELETE d
                """, path=path)

    def delete_repository_from_graph(self, repo_locator: str) -> bool:
        """Deletes a repository and all its contents from the graph. Returns True if deleted, False if not found."""
        repo_path_str = repo_locator
        try:
            p = Path(repo_locator)
            if p.exists() and p.is_dir():
                repo_path_str = self._get_virtual_repo_name(p.resolve())
        except Exception:
            pass
            
        with self.driver.session() as session:
            # Check if it exists
            result = session.run("MATCH (r:Repository {path: @path}) RETURN count(r) as cnt", path=repo_path_str).single()
            if not result or result["cnt"] == 0:
                warning_logger(f"Attempted to delete non-existent repository: {repo_path_str}")
                return False

            session.run("""MATCH (e)
                          WHERE e.path = @path OR STARTS_WITH(e.path, @path)
                          DETACH DELETE e""", path=repo_path_str)
            info_logger(f"Deleted repository and its contents from graph: {repo_path_str}")
            return True

    def _get_virtual_repo_name(self, path: Path) -> str:
        virtual_repo_name = f"local://{path.name}"
        branch = "detached"
        try:
            import git
            repo = git.Repo(path, search_parent_directories=True)
            if not repo.bare:
                try:
                    origin_url = next(iter(repo.remotes.origin.urls))
                    if origin_url.startswith("git@"):
                        origin_url = origin_url.replace(":", "/").replace("git@", "")
                    if origin_url.endswith(".git"):
                        origin_url = origin_url[:-4]
                    if origin_url.startswith("https://"):
                        origin_url = origin_url[8:]
                except Exception:
                    origin_url = f"local://{path.name}"
                try:
                    branch = repo.active_branch.name
                except TypeError:
                    branch = "detached"
                
                if not origin_url.startswith("local://"):
                    virtual_repo_name = f"{origin_url}@{branch}"
        except Exception:
            pass
        return virtual_repo_name

    def update_file_in_graph(self, path: Path, repo_path: Path, imports_map: dict):
        """Updates a single file's nodes in the graph."""
        virtual_repo_name = self._get_virtual_repo_name(repo_path)
        file_uri = to_global_uri(path, repo_path, virtual_repo_name)
        
        self.delete_file_from_graph(file_uri)

        if path.exists():
            file_data = self.parse_file(repo_path, path)
            
            def rewrite_paths(data, root, v_repo_name):
                if isinstance(data, dict):
                    for k, v in data.items():
                        if k in ('path', 'repo_path', 'target_file', 'caller_file', 'callee_file', 'called_file', 'resolved_parent_file_path') and isinstance(v, str):
                            data[k] = to_global_uri(Path(v), root, v_repo_name)
                        else:
                            rewrite_paths(v, root, v_repo_name)
                elif isinstance(data, list):
                    for item in data:
                        rewrite_paths(item, root, v_repo_name)
                        
            rewrite_paths(file_data, repo_path, virtual_repo_name)

            if "error" not in file_data:
                self.add_file_to_graph(file_data, virtual_repo_name, imports_map)
                return file_data
            else:
                error_logger(f"Skipping graph add for {file_uri} due to parsing error: {file_data['error']}")
                return None
        else:
            return {"deleted": True, "path": file_uri}

    def parse_file(self, repo_path: Path, path: Path, is_dependency: bool = False) -> Dict:
        """Parses a file with the appropriate language parser and extracts code elements."""
        parser = self.get_parser(path.suffix)
        if not parser:
            warning_logger(f"No parser found for file extension {path.suffix}. Skipping {path}")
            return {"path": str(path), "error": f"No parser for {path.suffix}"}

        debug_log(f"[parse_file] Starting parsing for: {path} with {parser.language_name} parser")
        try:
            index_source = (get_config_value("INDEX_SOURCE") or "false").lower() == "true"
            if parser.language_name == 'python':
                is_notebook = path.suffix == '.ipynb'
                file_data = parser.parse(
                    path,
                    is_dependency,
                    is_notebook=is_notebook,
                    index_source=index_source
                )
            else:
                file_data = parser.parse(
                    path,
                    is_dependency,
                    index_source=index_source
                )
            file_data['repo_path'] = str(repo_path)
            return file_data
        except Exception as e:
            error_logger(f"Error parsing {path} with {parser.language_name} parser: {e}")
            debug_log(f"[parse_file] Error parsing {path}: {e}")
            return {"path": str(path), "error": str(e)}

    def estimate_processing_time(self, path: Path) -> Optional[Tuple[int, float]]:
        """Estimate processing time and file count"""
        try:
            supported_extensions = self.parsers.keys()
            if path.is_file():
                if path.suffix in supported_extensions:
                    files = [path]
                else:
                    return 0, 0.0 # Not a supported file type
            else:
                all_files = path.rglob("*")
                files = [f for f in all_files if f.is_file() and f.suffix in supported_extensions]

                # Filter default ignored directories
                ignore_dirs_str = get_config_value("IGNORE_DIRS") or ""
                if ignore_dirs_str:
                    ignore_dirs = {d.strip().lower() for d in ignore_dirs_str.split(',') if d.strip()}
                    if ignore_dirs:
                        kept_files = []
                        for f in files:
                            try:
                                parts = set(p.lower() for p in f.relative_to(path).parent.parts)
                                if not parts.intersection(ignore_dirs):
                                    kept_files.append(f)
                            except ValueError:
                                kept_files.append(f)
                        files = kept_files
            
            total_files = len(files)
            estimated_time = total_files * 0.05 # tree-sitter is faster
            return total_files, estimated_time
        except Exception as e:
            error_logger(f"Could not estimate processing time for {path}: {e}")
            return None

    async def _build_graph_from_scip(
        self, path: Path, is_dependency: bool, job_id: Optional[str], lang: str
    ):
        """
        SCIP-based indexing path. Activated only when SCIP_INDEXER=true and
        a scip-<lang> binary is available.

        Steps:
          1. Run scip-<lang> CLI → index.scip
          2. Parse index.scip → nodes + reference edges
          3. Write nodes to graph (same MERGE queries as Tree-sitter path)
          4. Tree-sitter supplement: add source text + cyclomatic_complexity
          5. Write SCIP CALLS edges (precise, no heuristics)
        """
        import tempfile
        from .scip_indexer import ScipIndexer, ScipIndexParser
        from .graph_builder import TreeSitterParser  # supplement pass

        if job_id:
            self.job_manager.update_job(job_id, status=JobStatus.RUNNING)

        self.add_repository_to_graph(path, is_dependency)
        repo_name = path.name

        try:
            # Step 1: Run SCIP indexer
            with tempfile.TemporaryDirectory(prefix="cgc_scip_") as tmpdir:
                scip_file = ScipIndexer().run(path, lang, Path(tmpdir))

                if not scip_file:
                    warning_logger(
                        f"SCIP indexer produced no output for {path}. "
                        "Falling back to Tree-sitter."
                    )
                    # Hand off to Tree-sitter pipeline by re-calling without SCIP flag
                    # (the flag is checked at the start; override is not needed because
                    # we return here — caller will not re-enter this branch)
                    raise RuntimeError("SCIP produced no index — triggering Tree-sitter fallback")

                # Step 2: Parse index.scip
                scip_data = ScipIndexParser().parse(scip_file, path)
            
            if not scip_data:
                raise RuntimeError("SCIP parse returned empty result")

            files_data = scip_data.get("files", {})
            file_paths = [Path(p) for p in files_data.keys() if Path(p).exists()]
            
            # Step 3: Pre-scan for imports to correctly associate external modules/classes
            imports_map = self._pre_scan_for_imports(file_paths)

            if job_id:
                self.job_manager.update_job(job_id, total_files=len(files_data))

            # Step 4: Write nodes to graph using existing add_file_to_graph()
            processed = 0
            index_root = path.resolve()
            for abs_path_str, file_data in files_data.items():
                file_path = Path(abs_path_str)
                if file_path.is_file() and file_path_has_ignore_dir_segment(file_path, index_root):
                    continue
                file_data["repo_path"] = str(index_root)
                if job_id:
                    self.job_manager.update_job(job_id, current_file=abs_path_str)

                # Step 5: Tree-sitter supplement — add source text, complexity, imports and bases
                ts_parser = self.get_parser(file_path.suffix)
                if file_path.exists() and ts_parser:
                    try:
                        ts_data = ts_parser.parse(file_path, is_dependency, index_source=True)
                        if "error" not in ts_data:
                            # 1. Functions: complexity, source, decorators
                            ts_funcs = {f["name"]: f for f in ts_data.get("functions", [])}
                            for f in file_data.get("functions", []):
                                ts_f = ts_funcs.get(f["name"])
                                if ts_f:
                                    f.update({
                                        "source": ts_f.get("source"),
                                        "cyclomatic_complexity": ts_f.get("cyclomatic_complexity", 1),
                                        "decorators": ts_f.get("decorators", [])
                                    })
                            
                            # 2. Classes: bases (inheritance)
                            ts_classes = {c["name"]: c for c in ts_data.get("classes", [])}
                            for c in file_data.get("classes", []):
                                ts_c = ts_classes.get(c["name"])
                                if ts_c:
                                    c["bases"] = ts_c.get("bases", [])
                            
                            # 3. Imports: critical for cross-file resolution
                            file_data["imports"] = ts_data.get("imports", [])
                            
                            # 4. Variables/Other: value, etc.
                            file_data["variables"] = ts_data.get("variables", [])
                    except Exception as e:
                        debug_log(f"Tree-sitter supplement failed for {abs_path_str}: {e}")

                self.add_file_to_graph(file_data, repo_name, imports_map)

                processed += 1
                if job_id:
                    self.job_manager.update_job(job_id, processed_files=processed)
                await asyncio.sleep(0.01)

            # Step 6: Create INHERITS relationships (Supplemented from Tree-sitter)
            self._create_all_inheritance_links(list(files_data.values()), imports_map)

            # Step 7: Write SCIP CALLS edges — precise cross-file resolution
            with self.driver.session() as session:
                for file_data in files_data.values():
                    for edge in file_data.get("function_calls_scip", []):
                        try:
                            # Use line numbers for precise matching in case of duplicates
                            session.run("""
                                MATCH (caller:Function {name: @caller_name, path: @caller_file, line_number: @caller_line})
                                MATCH (callee:Function {name: @callee_name, path: @callee_file, line_number: @callee_line})
                                MERGE (caller)-[:CALLS {line_number: @ref_line, source: 'scip'}]->(callee)
                            """,
                            caller_name=self._name_from_symbol(edge["caller_symbol"]),
                            caller_file=edge["caller_file"],
                            caller_line=edge["caller_line"],
                            callee_name=edge["callee_name"],
                            callee_file=edge["callee_file"],
                            callee_line=edge["callee_line"],
                            ref_line=edge["ref_line"],
                            )
                        except Exception:
                            pass  # best-effort: node might not be indexed yet

            if job_id:
                self.job_manager.update_job(job_id, status=JobStatus.COMPLETED, end_time=datetime.now())

        except RuntimeError as e:
            # Graceful fallback to Tree-sitter when SCIP fails
            warning_logger(f"SCIP path failed ({e}), re-running with Tree-sitter...")
            # Temporarily disable the flag in-memory so the recursive call goes straight to TS
            # (we do this by calling the internal Tree-sitter steps directly)
            if job_id:
                self.job_manager.update_job(job_id, status=JobStatus.RUNNING)
            # Re-enter the async flow without SCIP check — handled by caller returning early
            # For simplicity, we just let the exception propagate to the outer handler so the
            # job is marked FAILED with a meaningful message rather than silently degrading.
            raise

        except Exception as e:
            error_logger(f"SCIP indexing failed for {path}: {e}")
            if job_id:
                self.job_manager.update_job(
                    job_id, status=JobStatus.FAILED, end_time=datetime.now(), errors=[str(e)]
                )

    def _name_from_symbol(self, symbol: str) -> str:
        """Extract human-readable name from a SCIP symbol ID string."""
        import re
        s = symbol.rstrip(".#")
        s = re.sub(r"\(\)\.?$", "", s) # Remove trailing () or ().
        parts = re.split(r'[/#]', s)
        last = parts[-1] if parts else symbol
        return last or symbol


    async def build_graph_from_path_async(
        self, path: Path, is_dependency: bool = False, job_id: str = None
    ):
        """Builds graph from a directory or file path."""
        try:
            # ------------------------------------------------------------------
            # SCIP feature flag: SCIP_INDEXER=true in ~/.codegraphcontext/.env
            # When enabled (and the binary is installed), SCIP handles the
            # indexing for supported languages. SCIP_INDEXER=false (default)
            # means this entire block is a no-op and existing behaviour is kept.
            # ------------------------------------------------------------------
            scip_enabled = (get_config_value("SCIP_INDEXER") or "false").lower() == "true"
            if scip_enabled:
                from .scip_indexer import ScipIndexer, ScipIndexParser, detect_project_lang, is_scip_available
                scip_langs_str = get_config_value("SCIP_LANGUAGES") or "python,typescript,go,rust,java"
                scip_languages = [l.strip() for l in scip_langs_str.split(",") if l.strip()]
                detected_lang = detect_project_lang(path, scip_languages)

                if detected_lang and is_scip_available(detected_lang):
                    info_logger(f"SCIP_INDEXER=true — using SCIP for language: {detected_lang}")
                    await self._build_graph_from_scip(path, is_dependency, job_id, detected_lang)
                    return   # SCIP handled it; skip Tree-sitter pipeline below
                else:
                    if detected_lang:
                        warning_logger(
                            f"SCIP_INDEXER=true but scip-{detected_lang} binary not found. "
                            f"Falling back to Tree-sitter. Install it first."
                        )
                    else:
                        info_logger(
                            "SCIP_INDEXER=true but no SCIP-supported language detected. "
                            "Falling back to Tree-sitter."
                        )
            # ------------------------------------------------------------------
            # Existing Tree-sitter pipeline (unchanged)
            # ------------------------------------------------------------------
            if job_id:
                self.job_manager.update_job(job_id, status=JobStatus.RUNNING)
            
            # --- COMPUTE VIRTUAL REPO NAME VIA GIT ---
            virtual_repo_name = f"local://{path.name}"
            branch = "detached"
            try:
                import git
                repo = git.Repo(path, search_parent_directories=True)
                if not repo.bare:
                    if repo.is_dirty(untracked_files=True):
                        error_msg = f"Repository at {repo.working_dir} has uncommitted changes (not porcelain). Indexation declined."
                        print(f"[CGC] {error_msg}", flush=True)
                        if job_id:
                            self.job_manager.update_job(
                                job_id, status=JobStatus.FAILED, end_time=datetime.now(), errors=[error_msg]
                            )
                        return
                    try:
                        origin_url = next(iter(repo.remotes.origin.urls))
                        if origin_url.startswith("git@"):
                            origin_url = origin_url.replace(":", "/").replace("git@", "")
                        if origin_url.endswith(".git"):
                            origin_url = origin_url[:-4]
                        if origin_url.startswith("https://"):
                            origin_url = origin_url[8:]
                    except Exception:
                        origin_url = f"local://{path.name}"
                    try:
                        branch = repo.active_branch.name
                    except TypeError:
                        branch = "detached"
                    
                    if not origin_url.startswith("local://"):
                        virtual_repo_name = f"{origin_url}@{branch}"
            except Exception:
                pass
            
            print(f"[CGC] Computed virtual repo name -> {virtual_repo_name}", flush=True)
            repo_name = virtual_repo_name

            self.add_repository_to_graph(repo_name, repo_name, is_dependency)

            is_valid_git_repo = False
            files = []
            try:
                import git
                git_repo = git.Repo(path, search_parent_directories=True)
                if not git_repo.bare and path.is_dir():
                    is_valid_git_repo = True
            except Exception:
                pass

            if is_valid_git_repo:
                try:
                    # git ls-files returns paths relative to the repo root
                    raw_files = git_repo.git.ls_files(str(path.resolve())).splitlines()
                    repo_root_path = Path(git_repo.working_dir).resolve()
                    
                    for f_str in raw_files:
                        fp = repo_root_path / f_str
                        if fp.exists() and fp.is_file():
                            files.append(fp)
                    print(f"[CGC] Git fast-path selected: Found {len(files)} tracked files.", flush=True)
                except Exception as e:
                    print(f"[CGC] Git ls-files failed: {e}. Falling back to default rglob.", flush=True)
                    is_valid_git_repo = False
            
            if not is_valid_git_repo:
                # Search for .cgcignore upwards
                cgcignore_path = None
                # ignore_root is always the indexed path itself so that file paths
                # are matched relative to the project being indexed.  A parent
                # .cgcignore is still loaded (for monorepo support), but anchoring
                # to its directory would make patterns like "website/" incorrectly
                # filter out every file when indexing the website sub-directory.
                ignore_root = path.resolve() if path.is_dir() else path.resolve().parent
    
                # Start search from path (or parent if path is file)
                curr = ignore_root
    
                # Walk up looking for .cgcignore
                while True:
                    candidate = curr / ".cgcignore"
                    if candidate.exists():
                        cgcignore_path = candidate
                        debug_log(f"Found .cgcignore at {curr} (filtering relative to {ignore_root})")
                        break
                    if curr.parent == curr: # Root hit
                        break
                    curr = curr.parent
    
                spec = None
                if cgcignore_path:
                    with open(cgcignore_path) as f:
                        user_patterns = [line.strip() for line in f.read().splitlines() if line.strip() and not line.strip().startswith('#')]
                    ignore_patterns = DEFAULT_IGNORE_PATTERNS + user_patterns
                    spec = pathspec.PathSpec.from_lines('gitwildmatch', ignore_patterns)
                else:
                    # No .cgcignore found — create one in the project root with default patterns
                    # so the user can see and customize what's being ignored
                    project_root = path.resolve() if path.is_dir() else path.resolve().parent
                    new_cgcignore = project_root / ".cgcignore"
                    try:
                        cgcignore_content = "# Auto-generated by CodeGraphContext\n"
                        cgcignore_content += "# Default ignore patterns for binary/media files\n"
                        cgcignore_content += "# Add your own patterns below\n\n"
                        cgcignore_content += "\n".join(DEFAULT_IGNORE_PATTERNS) + "\n"
                        new_cgcignore.write_text(cgcignore_content)
                        info_logger(f"Created default .cgcignore at {new_cgcignore}")
                    except OSError as e:
                        warning_logger(f"Could not create .cgcignore at {new_cgcignore}: {e}")
                    spec = pathspec.PathSpec.from_lines('gitwildmatch', DEFAULT_IGNORE_PATTERNS)
    
                supported_extensions = self.parsers.keys()
                all_files = path.rglob("*") if path.is_dir() else [path]
    
                # Previously only files with supported extensions were indexed.
                # Updated to include all files so that unsupported file types
                # can still be represented as minimal File nodes in the graph.
                files = [f for f in all_files if f.is_file()]
    
                # Filter default ignored directories
                ignore_dirs_str = get_config_value("IGNORE_DIRS") or ""
                if ignore_dirs_str and path.is_dir():
                    ignore_dirs = {d.strip().lower() for d in ignore_dirs_str.split(',') if d.strip()}
                    if ignore_dirs:
                        kept_files = []
                        for f in files:
                            try:
                                # Check if any parent directory in the relative path is in ignore list
                                parts = set(p.lower() for p in f.relative_to(path).parent.parts)
                                if not parts.intersection(ignore_dirs):
                                    kept_files.append(f)
                                else:
                                    # debug_log(f"Skipping default ignored file: {f}")
                                    pass
                            except ValueError:
                                 kept_files.append(f)
                        files = kept_files
                
                if spec:
                    filtered_files = []
                    for f in files:
                        try:
                            # Match relative to the directory containing .cgcignore
                            rel_path = f.relative_to(ignore_root)
                            if not spec.match_file(str(rel_path)):
                                filtered_files.append(f)
                            else:
                                debug_log(f"Ignored file based on .cgcignore: {rel_path}")
                        except ValueError:
                            # Should not happen if ignore_root is a parent, but safety fallback
                            filtered_files.append(f)
                    files = filtered_files
            
            # --- GIT INCREMENTAL INDEXING ---
            print(f"[{path.name}] Starting Git Incremental Indexing Check...", flush=True)
            changed_files_for_index = None # if None, index everything in `files`
            try:
                import git
                try:
                    repo = git.Repo(path, search_parent_directories=True)
                    print(f"[CGC Git] Found repository at {repo.working_dir}", flush=True)
                    if not repo.bare:
                        # virtual_repo_name and branch are already computed at the start of the function

                        current_commit = repo.head.commit.hexsha
                        with self.driver.session() as session:
                            try:
                                result = session.run("MATCH (r:Repository {path: @path}) RETURN r.last_indexed_commit as commit", path=virtual_repo_name).single()
                                last_indexed_commit = result['commit'] if result else None
                            except Exception as e:
                                print(f"[CGC Git] Fallback to full sync. Spanner graph missing 'last_indexed_commit' property. Error: {str(e).splitlines()[0]}", flush=True)
                                last_indexed_commit = None
                        
                        if last_indexed_commit:
                            if last_indexed_commit != current_commit:
                                try:
                                    print(f"[CGC Git] Computing diff from {last_indexed_commit[:8]} to HEAD ({current_commit[:8]})...", flush=True)
                                    diff_index = repo.commit(last_indexed_commit).diff(repo.commit(current_commit))
                                    repo_root = Path(repo.working_dir).resolve()
                                    
                                    changed_files_for_index = set()
                                    deleted_files = set()
                                    for d in diff_index:
                                        if d.change_type == 'D':
                                            deleted_files.add(str((repo_root / d.a_path).resolve()))
                                        elif d.change_type == 'R':
                                            deleted_files.add(str((repo_root / d.a_path).resolve()))
                                            changed_files_for_index.add(str((repo_root / d.b_path).resolve()))
                                        else:
                                            if d.a_path:
                                                deleted_files.add(str((repo_root / d.a_path).resolve()))
                                            changed_files_for_index.add(str((repo_root / d.b_path).resolve()))
                                    
                                    print(f"[CGC Git] Incremental Plan:", flush=True)
                                    print(f"  - Repository: {virtual_repo_name}", flush=True)
                                    print(f"  - Branch:     {branch}", flush=True)
                                    print(f"  - Revisions:  {last_indexed_commit[:8]} -> {current_commit[:8]}", flush=True)
                                    print(f"  - [ADDED/MODIFIED] ({len(changed_files_for_index)} files):", flush=True)
                                    for f in list(changed_files_for_index)[:10]:
                                        try: print(f"      + {Path(f).relative_to(repo_root)}", flush=True)
                                        except ValueError: print(f"      + {f}", flush=True)
                                    if len(changed_files_for_index) > 10: print(f"      ... ({len(changed_files_for_index) - 10} more)", flush=True)
                                    
                                    print(f"  - [DELETED/REPLACED] ({len(deleted_files)} files to prune):", flush=True)
                                    for f in list(deleted_files)[:10]:
                                        try: print(f"      - {Path(f).relative_to(repo_root)}", flush=True)
                                        except ValueError: print(f"      - {f}", flush=True)
                                    if len(deleted_files) > 10: print(f"      ... ({len(deleted_files) - 10} more)", flush=True)
                                    
                                    for df in deleted_files:
                                        deleted_uri = to_global_uri(Path(df), repo_root, virtual_repo_name)
                                        self.delete_file_from_graph(deleted_uri)
                                except Exception as e:
                                    print(f"[CGC Git] Failed to compute diff from {last_indexed_commit[:8]}: {e} - falling back to full index", flush=True)
                                    changed_files_for_index = None
                            else:
                                print(f"[CGC Git] Repository {virtual_repo_name} already up-to-date at {current_commit[:8]}.", flush=True)
                                changed_files_for_index = set() # No newly modified files
                        
                        # Set current commit
                        with self.driver.session() as session:
                            try:
                                session.run({
                                    "type": "node_merge",
                                    "table": "Repository",
                                    "_params": {"path": virtual_repo_name, "last_indexed_commit": current_commit}
                                })
                            except Exception as e:
                                pass # Spanner Schema does not have last_indexed_commit yet
                except git.exc.InvalidGitRepositoryError as e:
                    print(f"[CGC Git] Invalid repo error: {e}", flush=True)
            except ImportError as e:
                print(f"[CGC Git] GitPython not installed or accessible: {e}", flush=True)
            except Exception as e:
                print(f"[CGC Git] Integration error: {e}", flush=True)

            if changed_files_for_index is not None:
                new_files = []
                for f in files:
                    if str(f.resolve()) in changed_files_for_index:
                        new_files.append(f)
                files = new_files
                print(f"[CGC Git] Reduced parsing targets to {len(files)} files.", flush=True)
            # --- END GIT INCREMENTAL INDEXING ---

            if job_id:
                self.job_manager.update_job(job_id, total_files=len(files))
            
            debug_log("Starting pre-scan to build imports map...")
            imports_map = self._pre_scan_for_imports(files)
            debug_log(f"Pre-scan complete. Found {len(imports_map)} definitions.")

            def rewrite_paths(data, root, v_repo_name):
                if isinstance(data, dict):
                    for k, v in data.items():
                        if k in ('path', 'repo_path', 'target_file', 'caller_file', 'callee_file', 'called_file', 'resolved_parent_file_path') and isinstance(v, str):
                            data[k] = to_global_uri(Path(v), root, v_repo_name)
                        else:
                            rewrite_paths(v, root, v_repo_name)
                elif isinstance(data, list):
                    for item in data:
                        rewrite_paths(item, root, v_repo_name)

            all_file_data = []

            processed_count = 0
            import time
            global_batch = []
            
            with self.driver.session() as session:
                for file in files:
                    if file.is_file():
                        if job_id:
                            self.job_manager.update_job(job_id, current_file=str(file))
                        repo_path = path.resolve() if path.is_dir() else file.parent.resolve()
                        
                        file_data = self.parse_file(repo_path, file, is_dependency)
                        
                        rewrite_paths(file_data, repo_path, virtual_repo_name)
                        
                        # Previously only files with supported extensions were indexed.
                        # Updated to include all files so that unsupported file types
                        # can still be represented as minimal File nodes in the graph.
                        if "error" not in file_data:
                            try:
                                self.add_file_to_graph(file_data, repo_name, imports_map, global_batch=global_batch)
                            except Exception as file_err:
                                # Re-raise with the offending file path so the user
                                # can identify which source file triggered the error.
                                raise RuntimeError(
                                    f"{file_err} (while indexing file: {file})"
                                ) from file_err
                            all_file_data.append(file_data)
    
                        # Previously only files with supported extensions were indexed.
                        # Updated to include all files so that unsupported file types
                        # can still be represented as minimal File nodes in the graph.
                        else:
                            # create minimal node if parser not available
                            self.add_minimal_file_node(file, repo_path, virtual_repo_name, is_dependency, global_batch=global_batch)
                        processed_count += 1
                        
                        if len(global_batch) >= 1000:
                            if hasattr(session, 'run_batch'):
                                session.run_batch(global_batch)
                            else:
                                for query in global_batch:
                                    session.run(query)
                            global_batch.clear()
    
                        if job_id:
                            self.job_manager.update_job(job_id, processed_files=processed_count)
                        await asyncio.sleep(0.01)

                if global_batch:
                    if hasattr(session, 'run_batch'):
                        session.run_batch(global_batch)
                    else:
                        for query in global_batch:
                            session.run(query)
                    global_batch.clear()

            self._populate_resolution_cache(all_file_data)
            self._create_all_inheritance_links(all_file_data, imports_map)
            self._create_all_function_calls(all_file_data, imports_map)
            
            if job_id:
                self.job_manager.update_job(job_id, status=JobStatus.COMPLETED, end_time=datetime.now())
        except Exception as e:
            error_message = str(e)
            error_logger(f"Failed to build graph for path {path}: {error_message}")
            if job_id:
                # Only cancel if the directory itself was deleted during indexing
                if isinstance(e, FileNotFoundError) or isinstance(e, NotADirectoryError):
                    status = JobStatus.CANCELLED
                else:
                    status = JobStatus.FAILED

                self.job_manager.update_job(
                    job_id, status=status, end_time=datetime.now(), errors=[error_message]
                )

    # Create a minimal File node for unsupported file types.
    # These files do not contain parsed entities but should still
    # appear in the repository graph as requested in issue #707.
    def add_minimal_file_node(self, file_path: Path, repo_path: Path, virtual_repo_name: str, is_dependency: bool = False, global_batch: list = None):
        """Adds a minimal representation of a file (e.g., Markdown, Makefiles, images)
           to the graph so its parent folders and path are structurally captured even
           if no deep AST parsing is available."""
        file_uri = to_global_uri(file_path, repo_path, virtual_repo_name)
        repo_uri = to_global_uri(repo_path, repo_path, virtual_repo_name)
        
        if file_uri.startswith(repo_uri + "$"):
            relative_path = file_uri[len(repo_uri)+1:]
        else: # Safety fallback
            relative_path = file_path.name
            
        file_name = Path(relative_path).name
        
        import contextlib
        @contextlib.contextmanager
        def get_session():
            if global_batch is not None:
                yield None
            else:
                with self.driver.session() as s:
                    yield s

        with get_session() as session:
            batch_queries = global_batch if global_batch is not None else []
            def execute_or_queue(payload: dict):
                if session is not None:
                    session.run(payload)
                else:
                    batch_queries.append(payload)

            execute_or_queue({
                "type": "node_merge",
                "table": "Repository",
                "_params": {
                    "path": repo_uri,
                    "name": virtual_repo_name
                }
            })

            execute_or_queue({
                "type": "node_merge",
                "table": "File",
                "_params": {
                    "path": file_uri,
                    "name": file_name,
                    "relative_path": relative_path,
                    "is_dependency": is_dependency
                }
            })

            execute_or_queue({
                "type": "edge_merge",
                "edge_label": "CONTAINS",
                "sql_params": {
                    "id": str(uuid.uuid5(uuid.NAMESPACE_OID, f"CONTAINS_{repo_uri}_{file_uri}"))
                },
                "match_lookups": {
                    "repo": {
                        "table": "Repository",
                        "pk": "path",
                        "criteria": [["path", "param", "repo_path"]]
                    },
                    "file": {
                        "table": "File",
                        "pk": "path",
                        "criteria": [["path", "param", "file_path"]]
                    }
                },
                "original_parameters": {
                    "repo_path": repo_uri,
                    "file_path": file_uri
                },
                "src_var": "repo",
                "dst_var": "file",
                "src_pk": "path",
                "dst_pk": "path",
                "edge_props_raw": ""
            })

            # Establish directory structure uniformly using URIs
            parent_path = repo_uri
            parent_label = 'Repository'
            
            parts = Path(relative_path).parts[:-1]

            for part in parts:
                if parent_label == 'Repository':
                    current_path_str = f"{parent_path}${part}"
                else:
                    current_path_str = f"{parent_path}/{part}"
                
                execute_or_queue({
                    "type": "node_merge",
                    "table": "Directory",
                    "_params": {"path": current_path_str, "name": part}
                })

                execute_or_queue({
                    "type": "edge_merge",
                    "edge_label": "CONTAINS",
                    "sql_params": {
                        "id": str(uuid.uuid5(uuid.NAMESPACE_OID, f"CONTAINS_{parent_path}_{current_path_str}"))
                    },
                    "match_lookups": {
                        "parent": {
                            "table": parent_label,
                            "pk": "path",
                            "criteria": [["path", "param", "parent_path"]]
                        },
                        "child": {
                            "table": "Directory",
                            "pk": "path",
                            "criteria": [["path", "param", "current_path"]]
                        }
                    },
                    "original_parameters": {
                        "parent_path": parent_path,
                        "current_path": current_path_str
                    },
                    "src_var": "parent",
                    "dst_var": "child",
                    "src_pk": "path",
                    "dst_pk": "path",
                    "edge_props_raw": ""
                })

                parent_path = current_path_str
                parent_label = 'Directory'

            # Finally, connect the last directory to the file
            execute_or_queue({
                "type": "edge_merge",
                "edge_label": "CONTAINS",
                "sql_params": {
                    "id": str(uuid.uuid5(uuid.NAMESPACE_OID, f"CONTAINS_{parent_path}_{file_uri}"))
                },
                "match_lookups": {
                    "parent": {
                        "table": parent_label,
                        "pk": "path",
                        "criteria": [["path", "param", "parent_path"]]
                    },
                    "child": {
                        "table": "File",
                        "pk": "path",
                        "criteria": [["path", "param", "file_path"]]
                    }
                },
                "original_parameters": {
                    "parent_path": parent_path,
                    "file_path": file_uri
                },
                "src_var": "parent",
                "dst_var": "child",
                "src_pk": "path",
                "dst_pk": "path",
                "edge_props_raw": ""
            })

            if global_batch is None and session and batch_queries:
                if hasattr(session, 'run_batch'):
                    session.run_batch(batch_queries)
                else:
                    for query in batch_queries:
                        session.run(query)
                batch_queries.clear()
