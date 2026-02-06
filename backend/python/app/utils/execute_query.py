"""Execute SQL Query Tool for chatbot agent.

This module provides a tool for executing SQL queries against external data sources
like PostgreSQL and Snowflake. The tool takes a SQL query and source name,
determines the appropriate client to use, executes the query, and returns
results as markdown.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, TYPE_CHECKING

from langchain_core.tools import tool
from pydantic import BaseModel, Field

from app.utils.logger import create_logger

if TYPE_CHECKING:
    from app.config.configuration_service import ConfigurationService
    from app.connectors.services.base_arango_service import BaseArangoService

logger = create_logger("execute_query")


class ExecuteQueryArgs(BaseModel):
    """Required tool args for executing SQL queries."""
    
    query: str = Field(
        ...,
        description="The SQL query to execute against the data source."
    )
    source_name: str = Field(
        ...,
        description="Name of the data source to query (e.g., 'PostgreSQL', 'Snowflake'). Case-insensitive."
    )
    reason: str = Field(
        default="Executing SQL query to retrieve data",
        description="Why this query is needed to answer the user's question."
    )


def _detect_source_type(source_name: str) -> str:
    """Detect the source type from the source name (case-insensitive).
    
    Args:
        source_name: Name of the data source (e.g., 'PostgreSQL', 'SNOWFLAKE', etc.)
        
    Returns:
        Normalized source type: 'postgres' or 'snowflake' or 'unknown'
    """
    source_lower = source_name.lower()
    
    if "postgres" in source_lower:
        return "postgres"
    elif "snowflake" in source_lower:
        return "snowflake"
    else:
        return "unknown"


def _results_to_markdown(columns: List[str], rows: List[tuple]) -> str:
    """Convert query results to a markdown table.
    
    Args:
        columns: List of column names
        rows: List of row tuples
        
    Returns:
        Markdown formatted table string
    """
    if not columns:
        return "_No results returned._"
    
    if not rows:
        return f"_Query executed successfully but returned no rows._\n\nColumns: {', '.join(columns)}"
    
    # Limit rows for readability
    max_rows = 100
    truncated = len(rows) > max_rows
    display_rows = rows[:max_rows]
    
    # Build markdown table
    lines = []
    
    # Header row
    header = "| " + " | ".join(str(col) for col in columns) + " |"
    lines.append(header)
    
    # Separator row
    separator = "| " + " | ".join("---" for _ in columns) + " |"
    lines.append(separator)
    
    # Data rows
    for row in display_rows:
        # Handle None values and escape pipe characters
        formatted_cells = []
        for cell in row:
            if cell is None:
                formatted_cells.append("NULL")
            else:
                # Convert to string and escape pipes
                cell_str = str(cell).replace("|", "\\|")
                # Truncate very long values
                if len(cell_str) > 100:
                    cell_str = cell_str[:97] + "..."
                formatted_cells.append(cell_str)
        
        line = "| " + " | ".join(formatted_cells) + " |"
        lines.append(line)
    
    result = "\n".join(lines)
    
    if truncated:
        result += f"\n\n_Showing {max_rows} of {len(rows)} total rows._"
    else:
        result += f"\n\n_Total: {len(rows)} rows._"
    
    return result


async def _execute_postgres_query(
    query: str,
    config_service: "ConfigurationService",
    connector_instance_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Execute a query against PostgreSQL.
    
    Args:
        query: SQL query to execute
        config_service: Configuration service for retrieving connection details
        connector_instance_id: Optional connector instance ID
        
    Returns:
        Dict with 'ok', 'columns', 'rows' or 'error'
    """
    try:
        from app.sources.client.postgres.postgres import PostgreSQLClientBuilder
        
        client_builder = await PostgreSQLClientBuilder.build_from_services(
            logger=logger,
            config_service=config_service,
            connector_instance_id=connector_instance_id,
        )
        
        client = client_builder.get_client()
        
        with client:
            columns, rows = client.execute_query_raw(query)
            return {
                "ok": True,
                "columns": columns,
                "rows": rows,
            }
            
    except Exception as e:
        logger.error(f"PostgreSQL query execution failed: {e}")
        return {
            "ok": False,
            "error": f"PostgreSQL query failed: {str(e)}"
        }


async def _execute_snowflake_query(
    query: str,
    config_service: "ConfigurationService",
    connector_instance_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Execute a query against Snowflake.
    
    Args:
        query: SQL query to execute
        config_service: Configuration service for retrieving connection details
        connector_instance_id: Optional connector instance ID
        
    Returns:
        Dict with 'ok', 'columns', 'rows' or 'error'
    """
    try:
        from app.sources.client.snowflake.snowflake import (
            SnowflakeSDKClient,
            SnowflakeClient,
            SnowflakeConnectorConfig,
        )
        
        # Get connection config
        config_dict = await SnowflakeClient._get_connector_config(
            logger=logger,
            config_service=config_service,
            connector_instance_id=connector_instance_id,
        )
        
        config = SnowflakeConnectorConfig.model_validate(config_dict)
        account_identifier = config.accountIdentifier
        warehouse = config.warehouse
        
        # Build SDK client based on auth type
        auth_config = config.auth
        auth_type = auth_config.authType.value if hasattr(auth_config.authType, 'value') else str(auth_config.authType)
        
        if auth_type == "PAT":
            # PAT auth - use SDK client with token
            pat_token = auth_config.patToken
            if not pat_token:
                return {
                    "ok": False,
                    "error": "PAT token not configured for Snowflake connector"
                }
            
            sdk_client = SnowflakeSDKClient(
                account_identifier=account_identifier,
                warehouse=warehouse,
                oauth_token=pat_token,  # PAT tokens work with oauth authenticator
            )
        elif auth_type == "OAUTH":
            # OAuth auth
            credentials = config.credentials
            if not credentials or not credentials.access_token:
                return {
                    "ok": False,
                    "error": "OAuth access token not configured for Snowflake connector"
                }
            
            sdk_client = SnowflakeSDKClient(
                account_identifier=account_identifier,
                warehouse=warehouse,
                oauth_token=credentials.access_token,
            )
        else:
            return {
                "ok": False,
                "error": f"Unsupported Snowflake auth type: {auth_type}"
            }
        
        with sdk_client:
            columns, rows = sdk_client.execute_query_raw(query)
            return {
                "ok": True,
                "columns": columns,
                "rows": rows,
            }
            
    except Exception as e:
        logger.error(f"Snowflake query execution failed: {e}")
        return {
            "ok": False,
            "error": f"Snowflake query failed: {str(e)}"
        }


async def _execute_query_impl(
    query: str,
    source_name: str,
    config_service: "ConfigurationService",
    arango_service: Optional["BaseArangoService"] = None,
    connector_instance_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Main implementation for executing SQL queries.
    
    Args:
        query: SQL query to execute
        source_name: Name of the data source
        config_service: Configuration service
        arango_service: Optional ArangoDB service for looking up connector details
        connector_instance_id: Optional connector instance ID
        
    Returns:
        Dict with 'ok', 'markdown_result' or 'error'
    """
    source_type = _detect_source_type(source_name)
    
    if source_type == "unknown":
        return {
            "ok": False,
            "error": f"Unknown data source type: '{source_name}'. Supported types: PostgreSQL, Snowflake."
        }
    
    logger.info(f"Executing {source_type} query: {query[:100]}...")
    
    # Execute query based on source type
    if source_type == "postgres":
        result = await _execute_postgres_query(
            query=query,
            config_service=config_service,
            connector_instance_id=connector_instance_id,
        )
    elif source_type == "snowflake":
        result = await _execute_snowflake_query(
            query=query,
            config_service=config_service,
            connector_instance_id=connector_instance_id,
        )
    else:
        return {
            "ok": False,
            "error": f"Source type '{source_type}' is not yet implemented."
        }
    
    # Convert results to markdown
    if result.get("ok"):
        columns = result.get("columns", [])
        rows = result.get("rows", [])
        markdown = _results_to_markdown(columns, rows)
        
        return {
            "ok": True,
            "markdown_result": markdown,
            "row_count": len(rows),
            "column_count": len(columns),
        }
    else:
        return result


def create_execute_query_tool(
    config_service: "ConfigurationService",
    arango_service: Optional["BaseArangoService"] = None,
    connector_instance_id: Optional[str] = None,
) -> Callable:
    """Factory function to create the execute_query tool with runtime dependencies.
    
    Args:
        config_service: Configuration service for retrieving connection details
        arango_service: Optional ArangoDB service for connector lookups
        connector_instance_id: Optional connector instance ID
        
    Returns:
        A langchain tool for executing SQL queries
    """
    
    @tool("execute_sql_query", args_schema=ExecuteQueryArgs)
    async def execute_sql_query_tool(
        query: str,
        source_name: str,
        reason: str = "Executing SQL query to retrieve data",
    ) -> Dict[str, Any]:
        """Execute a SQL query against an external data source (PostgreSQL, Snowflake, etc.).
        
        Use this tool when you need to:
        - Query a database directly to retrieve specific data
        - Execute SQL queries provided in the context or generated based on table schemas
        - Get live data from connected data sources
        
        Args:
            query: The SQL query to execute (SELECT queries only for safety)
            source_name: Name of the data source (e.g., 'PostgreSQL', 'Snowflake')
            reason: Explanation of why this query is needed
            
        Returns:
            {
                "ok": true,
                "markdown_result": "| col1 | col2 |\\n|---|---|\\n| val1 | val2 |",
                "row_count": N,
                "column_count": M
            }
            or {"ok": false, "error": "..."}
        """
        try:
            result = await _execute_query_impl(
                query=query,
                source_name=source_name,
                config_service=config_service,
                arango_service=arango_service,
                connector_instance_id=connector_instance_id,
            )
            return result
        except Exception as e:
            logger.exception("execute_sql_query_tool failed")
            return {
                "ok": False,
                "error": f"Query execution failed: {str(e)}"
            }
    
    return execute_sql_query_tool
