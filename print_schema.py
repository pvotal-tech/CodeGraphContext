import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))
from codegraphcontext.core.database_spanner import NODE_TABLES, EDGE_TABLES

def generate_ddl():
    ddl_statements = []

    # 1. Create Node Tables
    for table_name, table_info in NODE_TABLES.items():
        ddl_statements.append(f"""CREATE TABLE IF NOT EXISTS `{table_name}` (
            {table_info['schema']}
        ) PRIMARY KEY ({table_info['pk']})""")

    # 2. Create physical Edge Tables
    for edge_table, table_info in EDGE_TABLES.items():
        ddl_statements.append(f"""CREATE TABLE IF NOT EXISTS `{edge_table}` (
            id STRING(36) NOT NULL,
            src_id STRING(MAX) NOT NULL,
            dst_id STRING(MAX) NOT NULL{table_info.get('properties', '')}
        ) PRIMARY KEY (id)""")

    # 3. Create Property Graph
    pg_ddl = ["CREATE PROPERTY GRAPH IF NOT EXISTS `CodeGraph`"]
    
    # 3a. Node Definitions
    pg_ddl.append("NODE TABLES (")
    node_lines = []
    for table_name, table_info in NODE_TABLES.items():
        node_lines.append(f"    `{table_name}` KEY ({table_info['pk']})")
    pg_ddl.append(",\n".join(node_lines))
    pg_ddl.append(")")

    # 3b. Edge Definitions (Polymorphic mapping)
    pg_ddl.append("EDGE TABLES (")
    edge_lines = []
    for edge_table, table_info in EDGE_TABLES.items():
        for src_lbl, dst_lbl in table_info['pairs']:
            edge_lines.append(f"    `{edge_table}` KEY (id) SOURCE KEY (src_id) REFERENCES `{src_lbl}` DESTINATION KEY (dst_id) REFERENCES `{dst_lbl}`")
    pg_ddl.append(",\n".join(edge_lines))
    pg_ddl.append(")")

    ddl_statements.append("\n".join(pg_ddl))

    for s in ddl_statements:
        print(s + ";\n")

if __name__ == "__main__":
    generate_ddl()
