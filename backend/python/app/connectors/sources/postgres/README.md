# PostgreSQL Connector

Clean implementation of PostgreSQL connector following the Snowflake pattern.

## Features

- **Single Database**: Connects to one PostgreSQL database
- **Schemas**: Syncs all schemas (filterable)
- **Tables**: Syncs tables with their metadata, columns, and row data
- **Foreign Keys**: Creates relationship edges between tables
- **Row Limit**: Configurable row limit via `POSTGRES_TABLE_ROW_LIMIT` env var (default: 50)
- **Full Sync Only**: Currently supports full synchronization only

## Architecture

### Hierarchy
```
Database (RecordGroup)
  └── Schema (RecordGroup)
       └── Table (Record - SQL_TABLE)
```

### Data Models

- `PostgresSchema`: Schema metadata
- `PostgresTable`: Table with columns, foreign keys, primary keys
- `ForeignKey`: Foreign key relationship between tables

### Filters

- **Sync Filters**:
  - Schemas (multiselect)
  - Tables (multiselect)
  
- **Indexing Filters**:
  - Index Tables (boolean)

## Configuration

### Authentication Fields
- `host`: PostgreSQL server host
- `port`: PostgreSQL server port (default: 5432)
- `database`: Database name (required)
- `user`: Username
- `password`: Password

### Environment Variables
- `POSTGRES_TABLE_ROW_LIMIT`: Maximum rows to fetch per table (default: 50)

## Implementation Details

- No separate data_fetcher.py - all logic in connector.py
- Uses existing `PostgreSQLDataSource` and `PostgreSQLClient`
- Follows same pipeline as Snowflake (SQL table parsers, etc.)
- Clean, minimal comments
- Org-level permissions (no user nodes)

## Data Format

Table data is streamed as JSON with:
- `table_name`
- `schema_name`
- `database_name`
- `columns` (with data types, constraints)
- `rows` (limited by POSTGRES_TABLE_ROW_LIMIT)
- `foreign_keys`

## Relations

- **Foreign Key Relations**: `FOREIGN_KEY` edges between tables
- **Inheritance**: Schemas inherit permissions from database, tables inherit from schemas
