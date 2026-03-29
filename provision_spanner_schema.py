import re

node_tables = [
    ("Repository", "path STRING, name STRING, is_dependency BOOL, PRIMARY KEY (path)"),
    ("File", "path STRING, name STRING, relative_path STRING, is_dependency BOOL, PRIMARY KEY (path)"),
    ("Directory", "path STRING, name STRING, PRIMARY KEY (path)"),
    ("Module", "name STRING, lang STRING, full_import_name STRING, PRIMARY KEY (name)"),
    ("Function", "uid STRING, name STRING, path STRING, line_number INT64, end_line INT64, source STRING, docstring STRING, lang STRING, cyclomatic_complexity INT64, context STRING, context_type STRING, class_context STRING, is_dependency BOOL, decorators ARRAY<STRING>, args ARRAY<STRING>, PRIMARY KEY (uid)"),
    ("Class", "uid STRING, name STRING, path STRING, line_number INT64, end_line INT64, source STRING, docstring STRING, lang STRING, is_dependency BOOL, decorators ARRAY<STRING>, PRIMARY KEY (uid)"),
    ("Variable", "uid STRING, name STRING, path STRING, line_number INT64, source STRING, docstring STRING, lang STRING, value STRING, context STRING, is_dependency BOOL, PRIMARY KEY (uid)"),
    ("Trait", "uid STRING, name STRING, path STRING, line_number INT64, end_line INT64, source STRING, docstring STRING, lang STRING, is_dependency BOOL, PRIMARY KEY (uid)"),
    ("Interface", "uid STRING, name STRING, path STRING, line_number INT64, end_line INT64, source STRING, docstring STRING, lang STRING, is_dependency BOOL, PRIMARY KEY (uid)"),
    ("Macro", "uid STRING, name STRING, path STRING, line_number INT64, end_line INT64, source STRING, docstring STRING, lang STRING, is_dependency BOOL, PRIMARY KEY (uid)"),
    ("Struct", "uid STRING, name STRING, path STRING, line_number INT64, end_line INT64, source STRING, docstring STRING, lang STRING, is_dependency BOOL, PRIMARY KEY (uid)"),
    ("Enum", "uid STRING, name STRING, path STRING, line_number INT64, end_line INT64, source STRING, docstring STRING, lang STRING, is_dependency BOOL, PRIMARY KEY (uid)"),
    ("Union", "uid STRING, name STRING, path STRING, line_number INT64, end_line INT64, source STRING, docstring STRING, lang STRING, is_dependency BOOL, PRIMARY KEY (uid)"),
    ("Annotation", "uid STRING, name STRING, path STRING, line_number INT64, end_line INT64, source STRING, docstring STRING, lang STRING, is_dependency BOOL, PRIMARY KEY (uid)"),
    ("Record", "uid STRING, name STRING, path STRING, line_number INT64, end_line INT64, source STRING, docstring STRING, lang STRING, is_dependency BOOL, PRIMARY KEY (uid)"),
    ("Property", "uid STRING, name STRING, path STRING, line_number INT64, end_line INT64, source STRING, docstring STRING, lang STRING, is_dependency BOOL, PRIMARY KEY (uid)"),
    ("Parameter", "uid STRING, name STRING, path STRING, function_line_number INT64, PRIMARY KEY (uid)")
]

node_pks = {}
for t, s in node_tables:
    m = re.search(r'PRIMARY KEY \((.*?)\)', s)
    if m:
        node_pks[t] = m.group(1).strip()

rel_tables = [
    ("CONTAINS", "FROM File TO Function, FROM File TO Class, FROM File TO Variable, FROM File TO Trait, FROM File TO Interface, FROM Macro TO Macro, FROM File TO Macro, FROM File TO Struct, FROM File TO Enum, FROM File TO Union, FROM File TO Annotation, FROM File TO Record, FROM File TO Property, FROM Repository TO Directory, FROM Directory TO Directory, FROM Directory TO File, FROM Repository TO File, FROM Class TO Function, FROM Function TO Function"),
    ("CALLS", "FROM Function TO Function, FROM Function TO Class, FROM File TO Function, FROM File TO Class, FROM Class TO Function, FROM Class TO Class, line_number INT64, args ARRAY<STRING>, full_call_name STRING"),
    ("IMPORTS", "FROM File TO Module, alias STRING, full_import_name STRING, imported_name STRING, line_number INT64"),
    ("INHERITS", "FROM Class TO Class, FROM Record TO Record, FROM Interface TO Interface"),
    ("HAS_PARAMETER", "FROM Function TO Parameter"),
    ("INCLUDES", "FROM Class TO Module"),
    ("IMPLEMENTS", "FROM Class TO Interface, FROM Struct TO Interface, FROM Record TO Interface")
]

nodes_ddl_list = []
edge_ddl_list = []
graph_nodes = []
graph_edges = []

for t, s in node_tables:
    s = s.replace("STRING", "STRING(MAX)")
    s = s.replace("ARRAY<STRING(MAX)>", "ARRAY<STRING(MAX)>")
    nodes_ddl_list.append(f"CREATE TABLE IF NOT EXISTS Node_{t} ({s});")
    graph_nodes.append(f"Node_{t} LABEL `{t}`")

for edge_name, edge_def in rel_tables:
    routes = []
    props = ""
    parts = edge_def.split(", ")
    for p in parts:
        if p.startswith("FROM"):
            m = re.match(r'FROM (\w+) TO (\w+)', p)
            if m:
                routes.append((m.group(1), m.group(2)))
        else:
            props += ", " + p.replace("STRING", "STRING(MAX)")

    for src, dst in routes:
        table_name = f"Edge_{edge_name}_{src}_{dst}"
        src_key = node_pks[src]
        dst_key = node_pks[dst]
        
        edge_table_def = f"src_{src_key} STRING(MAX), dst_{dst_key} STRING(MAX){props}, PRIMARY KEY (src_{src_key}, dst_{dst_key})"
        edge_ddl_list.append(f"CREATE TABLE IF NOT EXISTS {table_name} ({edge_table_def});")
        
        graph_edges.append(f"""
    {table_name}
      SOURCE KEY (src_{src_key}) REFERENCES Node_{src} ({src_key})
      DESTINATION KEY (dst_{dst_key}) REFERENCES Node_{dst} ({dst_key})
      LABEL `{edge_name}`
""")

for l in nodes_ddl_list: print(l)
for l in edge_ddl_list: print(l)

graph_ddl = f"""
CREATE OR REPLACE PROPERTY GRAPH CodeGraph
  NODE TABLES(
    {", ".join(graph_nodes)}
  )
  EDGE TABLES(
    {",".join(graph_edges)}
  );
"""
print(graph_ddl)
