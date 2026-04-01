import sys
from pathlib import Path

repo_root = Path("/Users/mguesnon/repositories/pvotal-infrastream-auto-dev-workspace/CodeGraphContext")

def to_global_uri(local_path: Path, repo_root: Path, virtual_repo_name: str) -> str:
    try:
        rel = local_path.resolve().relative_to(repo_root.resolve())
        if str(rel) == '.':
            return virtual_repo_name
        return f"{virtual_repo_name}${str(rel)}"
    except ValueError:
        return str(local_path.resolve())

v_repo_name = "github.com/pvotal-tech/CodeGraphContext@feature/branch_based_indexing"
file_path = repo_root / ".cgcignore"

file_uri = to_global_uri(file_path, repo_root, v_repo_name)
repo_uri = to_global_uri(repo_root, repo_root, v_repo_name)

print("file_uri:", file_uri)
print("repo_uri:", repo_uri)

if file_uri.startswith(repo_uri + "$"):
    relative_path = file_uri[len(repo_uri)+1:]
else:
    relative_path = Path(file_uri).name

print("relative_path:", repr(relative_path))
