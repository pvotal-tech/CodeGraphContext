from codegraphcontext.core.database_spanner import SpannerSessionWrapper
gql = """
MERGE (f:File {path: $path})
SET f.name = $name, f.relative_path = $relative_path, f.is_dependency = $is_dependency
"""
params = {"path": "p1", "name": "n1", "relative_path": ".cgcignore", "is_dependency": False}
wrapper = SpannerSessionWrapper(None, "my_graph")
sql, translated_params = wrapper._format_gql(gql, params)
print("SQL:")
print(sql)
print("PARAMS:")
print(translated_params)
