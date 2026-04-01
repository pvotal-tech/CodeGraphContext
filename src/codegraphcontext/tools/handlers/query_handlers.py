import re
import json
import urllib.parse
from pathlib import Path
import os
from datetime import datetime
from typing import Any, Dict
from ...utils.debug_log import debug_log

def execute_gql_query(db_manager, **args) -> Dict[str, Any]:
    """
    Tool implementation for executing a read-only Spanner GQL query.
    
    Important: Includes a safety check to prevent any database modification
    by disallowing keywords like CREATE, MERGE, DELETE, etc.
    """
    gql_query = args.get("gql_query")
    if not gql_query:
        # Fallback if the old name is passed
        gql_query = args.get("gql_query")
    if not gql_query:
        return {"error": "GQL query cannot be empty."}

    # Safety Check: Prevent any write operations to the database.
    # This check first removes all string literals and then checks for forbidden keywords.
    forbidden_keywords = ['CREATE', 'MERGE', 'DELETE', 'SET', 'REMOVE', 'DROP', 'CALL apoc', 'INSERT', 'UPDATE']
    
    # Regex to match single or double quoted strings, handling escaped quotes.
    string_literal_pattern = r'"(?:\\.|[^"\\])*"|\'(?:\\.|[^\'\\])*\''
    
    # Remove all string literals from the query.
    query_without_strings = re.sub(string_literal_pattern, '', gql_query)
    
    # Now, check for forbidden keywords in the query without strings.
    for keyword in forbidden_keywords:
        if re.search(r'\b' + keyword + r'\b', query_without_strings, re.IGNORECASE):
            return {
                "error": "This tool only supports read-only queries. Prohibited keywords like CREATE, MERGE, DELETE, SET, etc., are not allowed."
            }

    try:
        debug_log(f"Executing GQL query: {gql_query}")
        with db_manager.get_driver().session() as session:
            result = session.run(gql_query)
            # Convert results to a list of dictionaries for clean JSON serialization.
            records = [record.data() for record in result]
            
            return {
                "success": True,
                "query": gql_query,
                "record_count": len(records),
                "results": records
            }
    
    except Exception as e:
        import traceback
        import sys
        print(f"ERROR EXECUTING GQL: {gql_query}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        debug_log(f"Error executing Spanner GQL query: {str(e)}")
        return {
            "error": f"An unexpected error occurred while executing the query: {str(e)}",
            "details": traceback.format_exc()
        }

def visualize_graph_query(db_manager, **args) -> Dict[str, Any]:
    """Tool to generate a visualization URL for the local Playground UI."""
    gql_query = args.get("gql_query")
    if not gql_query:
        return {"error": "Cypher query cannot be empty."}

    try:
        # We point to the local server started by 'cgc visualize'
        # By default it runs on port 8000
        port = 8000
        encoded_query = urllib.parse.quote(gql_query)
        visualization_url = f"http://localhost:{port}/index.html?gql_query={encoded_query}"
        
        return {
            "success": True,
            "visualization_url": visualization_url,
            "message": "Click the URL to visualize this specific query in the Playground UI. (Ensure 'cgc visualize' is running)"
        }
    except Exception as e:
        debug_log(f"Error generating visualization URL: {str(e)}")
        return {"error": f"Failed to generate visualization URL: {str(e)}"}
