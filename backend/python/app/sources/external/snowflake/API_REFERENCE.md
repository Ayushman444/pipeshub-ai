# Snowflake Data Source API Reference

This document provides a comprehensive reference for all available APIs in the `SnowflakeDataSource` class.

## Table of Contents
- [Authentication](#authentication)
- [Databases](#databases)
- [Schemas](#schemas)
- [Tables](#tables)
- [Views](#views)
- [Stages](#stages)
- [Warehouses](#warehouses)
- [Users & Roles](#users--roles)
- [Grants (Permissions)](#grants-permissions)
- [Database Roles](#database-roles)
- [SQL Execution](#sql-execution)
- [File Operations](#file-operations)
- [Functions & Procedures](#functions--procedures)
- [Streams & Tasks](#streams--tasks)
- [Dynamic Tables](#dynamic-tables)
- [Notebooks](#notebooks)

---

## Authentication

The `SnowflakeDataSource` is initialized with a `SnowflakeClient`:

```python
from app.sources.client.snowflake.snowflake import SnowflakeClient, SnowflakeOAuthConfig
from app.sources.external.snowflake.snowflake_ import SnowflakeDataSource

config = SnowflakeOAuthConfig(
    account_identifier="your_account",
    oauth_token="your_oauth_token"
)
client = SnowflakeClient.build_with_config(config)
data_source = SnowflakeDataSource(client)
```

---

## Databases

### `list_databases()`
List all databases in the account.

```python
response = await data_source.list_databases(like="TEST%", show_limit=10)
```

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `like` | `str` | No | Filter by name pattern |
| `starts_with` | `str` | No | Filter by name prefix |
| `show_limit` | `int` | No | Maximum rows to return |
| `from_name` | `str` | No | Pagination cursor |

**Response:**
```json
{
  "data": [
    {"name": "TEST_DB", "kind": "STANDARD", "owner": "ACCOUNTADMIN", ...}
  ]
}
```

### `get_database(name)`
Get details of a specific database.

```python
response = await data_source.get_database(name="TEST_DB")
```

---

## Schemas

### `list_schemas(database)`
List all schemas in a database.

```python
response = await data_source.list_schemas(database="TEST_DB")
```

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `database` | `str` | Yes | Database name |
| `like` | `str` | No | Filter by name pattern |

**Response:**
```json
{
  "data": [
    {"name": "PUBLIC", "database_name": "TEST_DB", "owner": "ACCOUNTADMIN", ...}
  ]
}
```

### `get_schema(database, name)`
Get details of a specific schema.

---

## Tables

### `list_tables(database, schema)`
List all tables in a schema.

```python
response = await data_source.list_tables(database="TEST_DB", schema="PUBLIC")
```

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `database` | `str` | Yes | Database name |
| `schema` | `str` | Yes | Schema name |
| `like` | `str` | No | Filter by name pattern |

**Response:**
```json
{
  "data": [
    {"name": "CUSTOMERS", "kind": "TABLE", "columns": [...], ...}
  ]
}
```

### `get_table(database, schema, name)`
Get details of a specific table including columns.

---

## Views

### `list_views(database, schema)`
List all views in a schema.

```python
response = await data_source.list_views(database="TEST_DB", schema="PUBLIC")
```

### `get_view(database, schema, name)`
Get details of a specific view including the view definition.

---

## Stages

### `list_stages(database, schema)`
List all stages in a schema.

```python
response = await data_source.list_stages(database="TEST_DB", schema="PUBLIC")
```

**Response:**
```json
{
  "data": [
    {
      "name": "MY_STAGE",
      "kind": "PERMANENT",
      "url": "",
      "directory_table": {"enable": true, ...}
    }
  ]
}
```

### `get_stage(database, schema, name)`
Get details of a specific stage.

---

## Warehouses

### `list_warehouses()`
List all warehouses.

```python
response = await data_source.list_warehouses()
```

### `get_warehouse(name)`
Get details of a specific warehouse.

---

## Users & Roles

### `list_users()`
List all users in the account.

```python
response = await data_source.list_users(like="ADMIN%")
```

**Response:**
```json
{
  "data": [
    {"name": "ADMIN_USER", "login_name": "admin@company.com", "email": "admin@company.com", ...}
  ]
}
```

### `list_roles()`
List all roles in the account.

```python
response = await data_source.list_roles()
```

**Response:**
```json
{
  "data": [
    {"name": "ACCOUNTADMIN", "owner": "", "comment": "Account administrator can manage all objects in the account"}
  ]
}
```

---

## Grants (Permissions)

### `list_grants_to_role(role_name)`
List all privileges granted TO a role.

```python
response = await data_source.list_grants_to_role(role_name="DATA_ANALYST")
```

**Response:** (via REST API)
```json
{
  "data": [
    {"privilege": "USAGE", "granted_on": "DATABASE", "name": "TEST_DB", ...}
  ]
}
```

### `list_grants_to_user(user_name)`
List all roles granted to a user.

```python
response = await data_source.list_grants_to_user(user_name="john@company.com")
```

### `list_grants_of_role(role_name)`
List who has been granted this role (members).

```python
response = await data_source.list_grants_of_role(role_name="DATA_ANALYST")
```

**Response:** (via SQL: `SHOW GRANTS OF ROLE`)
```json
{
  "data": [
    ["2024-01-01", "DATA_ANALYST", "USER", "JOHN_DOE", "ACCOUNTADMIN"]
  ]
}
```

### `list_grants_on_object(object_type, object_name)`
List all grants on a specific object.

```python
response = await data_source.list_grants_on_object(
    object_type="DATABASE",
    object_name="TEST_DB"
)
```

---

## Database Roles

### `list_database_roles(database)`
List all database-scoped roles.

```python
response = await data_source.list_database_roles(database="TEST_DB")
```

### `get_database_role(database, name)`
Get details of a specific database role.

---

## SQL Execution

### `execute_sql(statement, ...)`
Execute arbitrary SQL statements.

```python
response = await data_source.execute_sql(
    statement="SELECT * FROM my_table LIMIT 10",
    database="TEST_DB",
    schema="PUBLIC",
    warehouse="COMPUTE_WH",
    timeout=60
)
```

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| `statement` | `str` | Yes | SQL statement to execute |
| `database` | `str` | No | Database context |
| `schema` | `str` | No | Schema context |
| `warehouse` | `str` | No | Warehouse for execution |
| `role` | `str` | No | Role to use |
| `timeout` | `int` | No | Query timeout in seconds |
| `async_exec` | `bool` | No | Execute asynchronously |

**Response:**
```json
{
  "statementHandle": "01abc123-...",
  "data": [["row1_col1", "row1_col2"], ["row2_col1", "row2_col2"]],
  "resultSetMetaData": {"numRows": 2, "format": "jsonv2", ...}
}
```

### `get_statement_status(statement_handle)`
Check status of an async statement.

```python
response = await data_source.get_statement_status(statement_handle="01abc123-...")
```

### `cancel_statement(statement_handle)`
Cancel a running statement.

---

## File Operations

### `list_stage_files(database, schema, stage, warehouse)`
List all files in a stage using Directory Table.

```python
response = await data_source.list_stage_files(
    database="TEST_DB",
    schema="PUBLIC",
    stage="MY_STAGE",
    warehouse="COMPUTE_WH"
)
```

**Response:**
```json
{
  "data": [
    ["sample.pdf", "2992", "2024-01-01 12:00:00", "abc123md5", "abc123etag", "https://.../api/files/..."]
  ]
}
```

Columns: `RELATIVE_PATH`, `SIZE`, `LAST_MODIFIED`, `MD5`, `ETAG`, `FILE_URL`

### `generate_presigned_url(database, schema, stage, file_path, ...)`
Generate a pre-signed URL for direct file download.

```python
response = await data_source.generate_presigned_url(
    database="TEST_DB",
    schema="PUBLIC",
    stage="MY_STAGE",
    file_path="folder/document.pdf",
    expiration_seconds=3600,
    warehouse="COMPUTE_WH"
)
```

**Response:**
```json
{
  "data": [["https://s3.amazonaws.com/...?X-Amz-Signature=..."]]
}
```

### `download_file(file_url)`
Download file via Snowflake Files API.

```python
response = await data_source.download_file(
    file_url="https://account.snowflakecomputing.com/api/files/DB/SCHEMA/STAGE/file.pdf"
)
```

**Response:**
```python
response.raw_content  # bytes - raw file content
```

### `download_file_from_presigned_url(presigned_url)`
Download file directly from pre-signed URL (S3/Azure/GCS).

```python
response = await data_source.download_file_from_presigned_url(
    presigned_url="https://s3.amazonaws.com/...?X-Amz-Signature=..."
)
```

**Response:**
```python
response.raw_content  # bytes - raw file content
```

---

## Functions & Procedures

### `list_functions(database, schema)`
List all user-defined functions.

```python
response = await data_source.list_functions(database="TEST_DB", schema="PUBLIC")
```

### `list_procedures(database, schema)`
List all stored procedures.

```python
response = await data_source.list_procedures(database="TEST_DB", schema="PUBLIC")
```

---

## Streams & Tasks

### `list_streams(database, schema)`
List all streams in a schema.

```python
response = await data_source.list_streams(database="TEST_DB", schema="PUBLIC")
```

### `list_tasks(database, schema)`
List all tasks in a schema.

```python
response = await data_source.list_tasks(database="TEST_DB", schema="PUBLIC")
```

---

## Dynamic Tables

### `list_dynamic_tables(database, schema)`
List all dynamic tables.

```python
response = await data_source.list_dynamic_tables(database="TEST_DB", schema="PUBLIC")
```

### `get_dynamic_table(database, schema, name)`
Get details of a dynamic table including refresh settings.

---

## Notebooks

### `list_notebooks(database, schema)`
List all notebooks.

```python
response = await data_source.list_notebooks(database="TEST_DB", schema="PUBLIC")
```

### `get_notebook(database, schema, name)`
Get details of a specific notebook.

---

## Response Format

All methods return a `SnowflakeResponse` object:

```python
class SnowflakeResponse:
    success: bool           # True if request succeeded
    data: Dict | List       # Response data (JSON)
    raw_content: bytes      # Binary content (for file downloads)
    error: str              # Error message if failed
    message: str            # Status message
    statement_handle: str   # Handle for async queries
```

**Example Usage:**
```python
response = await data_source.list_databases()

if response.success:
    databases = response.data.get("data", [])
    for db in databases:
        print(f"Database: {db['name']}")
else:
    print(f"Error: {response.error}")
```
