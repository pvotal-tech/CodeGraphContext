import os
import sys

# Add src to sys.path so we can import codegraphcontext
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

# First parse the ~/.codegraphcontext/.env file to export the env vars needed for Spanner
from codegraphcontext.cli.cgc import get_config_value
import codegraphcontext.cli.cgc as cgc

if __name__ == "__main__":
    from codegraphcontext.core.database_spanner import SpannerDBManager
    
    db_manager = SpannerDBManager()
    driver = db_manager.get_driver() # This initializes the connection and self._database_obj
    
    database = db_manager._database_obj
    
    print("Executing Spanner Deletion Script...")
    
    delete_ddl = [
        "DROP PROPERTY GRAPH IF EXISTS `CodeGraph`",
        "DROP TABLE IF EXISTS `CONTAINS`",
        "DROP TABLE IF EXISTS `CALLS`",
        "DROP TABLE IF EXISTS `IMPORTS`",
        "DROP TABLE IF EXISTS `INHERITS`",
        "DROP TABLE IF EXISTS `HAS_PARAMETER`",
        "DROP TABLE IF EXISTS `INCLUDES`",
        "DROP TABLE IF EXISTS `IMPLEMENTS`",
        "DROP TABLE IF EXISTS `Repository`",
        "DROP TABLE IF EXISTS `File`",
        "DROP TABLE IF EXISTS `Directory`",
        "DROP TABLE IF EXISTS `Module`",
        "DROP TABLE IF EXISTS `Function`",
        "DROP TABLE IF EXISTS `Class`",
        "DROP TABLE IF EXISTS `Variable`",
        "DROP TABLE IF EXISTS `Trait`",
        "DROP TABLE IF EXISTS `Interface`",
        "DROP TABLE IF EXISTS `Macro`",
        "DROP TABLE IF EXISTS `Struct`",
        "DROP TABLE IF EXISTS `Enum`",
        "DROP TABLE IF EXISTS `Union`",
        "DROP TABLE IF EXISTS `Annotation`",
        "DROP TABLE IF EXISTS `Record`",
        "DROP TABLE IF EXISTS `Property`",
        "DROP TABLE IF EXISTS `Parameter`",
    ]
    
    operation = database.update_ddl(delete_ddl)
    print("Waiting for operation to complete...")
    operation.result(120)
    print("Successfully dropped all CodeGraphContext tables!")
    
    print("Now re-provisioning Spanner tables based on python DDLs...")
    db_manager.provision_schema()
    print("Re-provisioning complete.")
