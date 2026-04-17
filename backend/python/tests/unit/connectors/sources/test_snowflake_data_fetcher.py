"""Tests for app.connectors.sources.snowflake.data_fetcher."""

import json
import logging
from dataclasses import asdict
from typing import Any, Optional
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.connectors.sources.snowflake.data_fetcher import (
    ForeignKey,
    SnowflakeDatabase,
    SnowflakeDataFetcher,
    SnowflakeFile,
    SnowflakeFolder,
    SnowflakeHierarchy,
    SnowflakeSchema,
    SnowflakeStage,
    SnowflakeTable,
    SnowflakeView,
)


# ===========================================================================
# Helpers
# ===========================================================================


def _response(success: bool = True, data: Any = None, error: Optional[str] = None):
    resp = MagicMock()
    resp.success = success
    resp.data = data if data is not None else {}
    resp.error = error
    return resp


def _make_fetcher(warehouse: Optional[str] = "WH") -> SnowflakeDataFetcher:
    ds = MagicMock()
    ds.list_databases = AsyncMock()
    ds.list_schemas = AsyncMock()
    ds.list_tables = AsyncMock()
    ds.list_views = AsyncMock()
    ds.list_stages = AsyncMock()
    ds.list_stage_files = AsyncMock()
    ds.execute_sql = AsyncMock()
    return SnowflakeDataFetcher(ds, warehouse=warehouse)


# ===========================================================================
# Dataclasses
# ===========================================================================


class TestSnowflakeDatabase:
    def test_defaults(self):
        db = SnowflakeDatabase(name="DB1")
        assert db.name == "DB1"
        assert db.owner is None
        assert db.comment is None
        assert db.created_at is None


class TestSnowflakeSchema:
    def test_fields(self):
        s = SnowflakeSchema(name="S1", database_name="DB1", owner="u", comment="c", created_at="t")
        assert s.name == "S1"
        assert s.database_name == "DB1"


class TestSnowflakeTable:
    def test_defaults(self):
        t = SnowflakeTable(name="T1", database_name="DB", schema_name="S")
        assert t.columns == []
        assert t.foreign_keys == []
        assert t.primary_keys == []

    def test_fqn(self):
        t = SnowflakeTable(name="T", database_name="DB", schema_name="S")
        assert t.fqn == "DB.S.T"

    def test_column_signature_empty(self):
        t = SnowflakeTable(name="T", database_name="DB", schema_name="S")
        assert t.column_signature == ""

    def test_column_signature_stable(self):
        cols1 = [{"name": "b", "data_type": "int"}, {"name": "a", "data_type": "str"}]
        cols2 = [{"name": "a", "data_type": "str"}, {"name": "b", "data_type": "int"}]
        t1 = SnowflakeTable(name="T", database_name="D", schema_name="S", columns=cols1)
        t2 = SnowflakeTable(name="T", database_name="D", schema_name="S", columns=cols2)
        assert t1.column_signature == t2.column_signature
        assert t1.column_signature != ""


class TestSnowflakeView:
    def test_fqn_and_defaults(self):
        v = SnowflakeView(name="V", database_name="D", schema_name="S")
        assert v.fqn == "D.S.V"
        assert v.is_secure is False
        assert v.source_tables == []


class TestSnowflakeStage:
    def test_fqn(self):
        s = SnowflakeStage(name="St", database_name="D", schema_name="S")
        assert s.fqn == "D.S.St"
        assert s.stage_type == "INTERNAL"


class TestSnowflakeFile:
    def test_file_name_simple(self):
        f = SnowflakeFile(relative_path="a.csv", stage_name="st", database_name="d", schema_name="s")
        assert f.file_name == "a.csv"
        assert f.parent_folder is None
        assert f.is_folder is False

    def test_file_name_nested(self):
        f = SnowflakeFile(relative_path="dir/sub/file.csv", stage_name="st", database_name="d", schema_name="s")
        assert f.file_name == "file.csv"
        assert f.parent_folder == "dir/sub"

    def test_is_folder(self):
        f = SnowflakeFile(relative_path="dir/", stage_name="st", database_name="d", schema_name="s")
        assert f.is_folder is True
        # parent_folder strips trailing slash, single segment => None
        assert f.parent_folder is None


class TestSnowflakeFolder:
    def test_name(self):
        fd = SnowflakeFolder(path="root/sub", stage_name="st", database_name="d", schema_name="s")
        assert fd.name == "sub"


class TestForeignKey:
    def test_fqns(self):
        fk = ForeignKey(
            constraint_name="fk1",
            database_name="DB",
            source_schema="S1",
            source_table="T1",
            source_column="a",
            target_schema="S2",
            target_table="T2",
            target_column="b",
        )
        assert fk.source_fqn == "DB.S1.T1"
        assert fk.target_fqn == "DB.S2.T2"


class TestSnowflakeHierarchy:
    def test_summary_and_to_dict(self):
        h = SnowflakeHierarchy()
        h.databases = [SnowflakeDatabase(name="DB")]
        h.schemas = {"DB": [SnowflakeSchema(name="S", database_name="DB")]}
        h.tables = {"DB.S": [SnowflakeTable(name="T", database_name="DB", schema_name="S")]}
        h.views = {"DB.S": [SnowflakeView(name="V", database_name="DB", schema_name="S")]}
        h.stages = {"DB.S": [SnowflakeStage(name="St", database_name="DB", schema_name="S")]}
        h.files = {
            "DB.S.St": [SnowflakeFile(relative_path="a", stage_name="St",
                                     database_name="DB", schema_name="S")]
        }
        h.folders = {
            "DB.S.St": [SnowflakeFolder(path="dir", stage_name="St",
                                       database_name="DB", schema_name="S")]
        }
        h.foreign_keys = [ForeignKey("fk", "DB", "S", "T", "c", "S", "T2", "c2")]

        summary = h.summary()
        assert summary["databases"] == 1
        assert summary["schemas"] == 1
        assert summary["tables"] == 1
        assert summary["views"] == 1
        assert summary["stages"] == 1
        assert summary["files"] == 1
        assert summary["folders"] == 1
        assert summary["foreign_keys"] == 1

        d = h.to_dict()
        assert "summary" in d
        assert d["databases"][0]["name"] == "DB"
        assert "DB.S" in d["tables"]


# ===========================================================================
# SnowflakeDataFetcher: _extract_items / _parse_sql_result
# ===========================================================================


class TestExtractItems:
    def test_none(self):
        f = _make_fetcher()
        assert f._extract_items(None) == []

    def test_list(self):
        f = _make_fetcher()
        assert f._extract_items([{"a": 1}]) == [{"a": 1}]

    def test_dict_data(self):
        f = _make_fetcher()
        assert f._extract_items({"data": [{"x": 1}]}) == [{"x": 1}]

    def test_dict_rowset(self):
        f = _make_fetcher()
        assert f._extract_items({"rowset": [{"x": 1}]}) == [{"x": 1}]

    def test_dict_items(self):
        f = _make_fetcher()
        assert f._extract_items({"items": [{"x": 1}]}) == [{"x": 1}]

    def test_unknown(self):
        f = _make_fetcher()
        assert f._extract_items({"other": []}) == []

    def test_scalar(self):
        f = _make_fetcher()
        assert f._extract_items(42) == []


class TestParseSqlResult:
    def test_empty(self):
        f = _make_fetcher()
        assert f._parse_sql_result(None) == []
        assert f._parse_sql_result([]) == []

    def test_list_rows_with_meta(self):
        f = _make_fetcher()
        data = {
            "resultSetMetaData": {"rowType": [{"name": "A"}, {"name": "B"}]},
            "data": [[1, 2], [3, 4]],
        }
        rows = f._parse_sql_result(data)
        assert rows == [{"A": 1, "B": 2}, {"A": 3, "B": 4}]

    def test_dict_rows(self):
        f = _make_fetcher()
        data = {
            "resultSetMetaData": {"rowType": []},
            "data": [{"x": 1}],
        }
        rows = f._parse_sql_result(data)
        assert rows == [{"x": 1}]

    def test_no_meta_fallback(self):
        f = _make_fetcher()
        data = {"data": [[1, 2]]}
        rows = f._parse_sql_result(data)
        assert rows == [{}]  # empty columns => zip produces empty dict


# ===========================================================================
# _fetch_databases / _fetch_schemas / _fetch_tables / _fetch_views / _fetch_stages
# ===========================================================================


@pytest.mark.asyncio
class TestFetchDatabases:
    async def test_returns_databases(self):
        f = _make_fetcher()
        f.data_source.list_databases.return_value = _response(
            True, [{"name": "DB1", "owner": "u", "comment": "c", "created_on": "t"}]
        )
        dbs = await f._fetch_databases()
        assert len(dbs) == 1
        assert dbs[0].name == "DB1"
        assert dbs[0].owner == "u"

    async def test_returns_empty_on_failure(self):
        f = _make_fetcher()
        f.data_source.list_databases.return_value = _response(False, error="oops")
        assert await f._fetch_databases() == []


@pytest.mark.asyncio
class TestFetchSchemas:
    async def test_returns_schemas(self):
        f = _make_fetcher()
        f.data_source.list_schemas.return_value = _response(
            True, [{"name": "S1"}, {"name": "S2"}]
        )
        schemas = await f._fetch_schemas("DB")
        assert [s.name for s in schemas] == ["S1", "S2"]

    async def test_uses_cache(self):
        f = _make_fetcher()
        f._schema_cache["DB"] = [SnowflakeSchema(name="cached", database_name="DB")]
        schemas = await f._fetch_schemas("DB")
        assert schemas[0].name == "cached"
        f.data_source.list_schemas.assert_not_called()

    async def test_empty_on_failure(self):
        f = _make_fetcher()
        f.data_source.list_schemas.return_value = _response(False, error="e")
        assert await f._fetch_schemas("DB") == []


@pytest.mark.asyncio
class TestFetchTables:
    async def test_returns_tables(self):
        f = _make_fetcher()
        f.data_source.list_tables.return_value = _response(
            True,
            [{
                "name": "T",
                "rows": 10,
                "bytes": 100,
                "owner": "u",
                "kind": "TABLE",
                "created_on": "c",
                "last_altered": "la",
            }],
        )
        tables = await f._fetch_tables("DB", "S")
        assert len(tables) == 1
        assert tables[0].row_count == 10
        assert tables[0].bytes == 100
        assert tables[0].table_type == "TABLE"
        assert tables[0].last_altered == "la"

    async def test_fallback_fields(self):
        f = _make_fetcher()
        f.data_source.list_tables.return_value = _response(
            True, [{"name": "T", "table_type": "VIEW", "changed_on": "cc"}]
        )
        tables = await f._fetch_tables("DB", "S")
        assert tables[0].table_type == "VIEW"
        assert tables[0].last_altered == "cc"

    async def test_empty_on_failure(self):
        f = _make_fetcher()
        f.data_source.list_tables.return_value = _response(False, error="err")
        assert await f._fetch_tables("DB", "S") == []


@pytest.mark.asyncio
class TestFetchViews:
    async def test_returns_views(self):
        f = _make_fetcher()
        f.data_source.list_views.return_value = _response(
            True, [{"name": "V", "is_secure": True, "text": "select 1"}]
        )
        views = await f._fetch_views("DB", "S")
        assert views[0].is_secure is True
        assert views[0].definition == "select 1"

    async def test_empty_on_failure(self):
        f = _make_fetcher()
        f.data_source.list_views.return_value = _response(False, error="e")
        assert await f._fetch_views("DB", "S") == []


@pytest.mark.asyncio
class TestFetchStages:
    async def test_internal(self):
        f = _make_fetcher()
        f.data_source.list_stages.return_value = _response(
            True, [{"name": "St", "type": "INTERNAL NAMED"}]
        )
        stages = await f._fetch_stages("DB", "S")
        assert stages[0].stage_type == "INTERNAL"

    async def test_external(self):
        f = _make_fetcher()
        f.data_source.list_stages.return_value = _response(
            True, [{"name": "St", "type": "EXTERNAL"}]
        )
        stages = await f._fetch_stages("DB", "S")
        assert stages[0].stage_type == "EXTERNAL"

    async def test_empty_on_failure(self):
        f = _make_fetcher()
        f.data_source.list_stages.return_value = _response(False, error="e")
        assert await f._fetch_stages("DB", "S") == []


# ===========================================================================
# _fetch_stage_files / _deduce_folders
# ===========================================================================


@pytest.mark.asyncio
class TestFetchStageFiles:
    async def test_no_warehouse_returns_empty(self):
        f = _make_fetcher(warehouse=None)
        assert await f._fetch_stage_files("DB", "S", "St") == []
        f.data_source.list_stage_files.assert_not_called()

    async def test_failure_returns_empty(self):
        f = _make_fetcher()
        f.data_source.list_stage_files.return_value = _response(False, error="x")
        assert await f._fetch_stage_files("DB", "S", "St") == []

    async def test_parses_file_rows(self):
        f = _make_fetcher()
        f.data_source.list_stage_files.return_value = _response(
            True,
            {"data": [
                ["dir/a.txt", "123", "2024-01-01", "md5abc", "cols", "http://u"],
                ["b.txt"],
            ]},
        )
        files = await f._fetch_stage_files("DB", "S", "St")
        assert len(files) == 2
        assert files[0].relative_path == "dir/a.txt"
        assert files[0].size == 123
        assert files[0].md5 == "md5abc"
        assert files[0].file_url == "http://u"
        assert files[1].size == 0

    async def test_data_not_dict(self):
        f = _make_fetcher()
        f.data_source.list_stage_files.return_value = _response(True, ["not", "dict"])
        assert await f._fetch_stage_files("DB", "S", "St") == []


class TestDeduceFolders:
    def test_empty(self):
        f = _make_fetcher()
        assert f._deduce_folders([], "DB", "S", "St") == []

    def test_single_file(self):
        f = _make_fetcher()
        files = [SnowflakeFile(relative_path="a/b/c.txt", stage_name="St",
                               database_name="DB", schema_name="S")]
        folders = f._deduce_folders(files, "DB", "S", "St")
        paths = sorted([fd.path for fd in folders])
        assert paths == ["a", "a/b"]
        # root folder has no parent
        root = next(fd for fd in folders if fd.path == "a")
        assert root.parent_path is None
        nested = next(fd for fd in folders if fd.path == "a/b")
        assert nested.parent_path == "a"

    def test_file_in_root_no_folders(self):
        f = _make_fetcher()
        files = [SnowflakeFile(relative_path="file.txt", stage_name="St",
                               database_name="DB", schema_name="S")]
        assert f._deduce_folders(files, "DB", "S", "St") == []


# ===========================================================================
# _fetch_all_columns_in_schema / get_table_ddl / foreign & primary keys
# ===========================================================================


@pytest.mark.asyncio
class TestFetchAllColumns:
    async def test_no_warehouse(self):
        f = _make_fetcher(warehouse=None)
        assert await f._fetch_all_columns_in_schema("DB", "S") == {}

    async def test_uses_cache(self):
        f = _make_fetcher()
        f._columns_cache["DB.S"] = {"T": [{"name": "col"}]}
        result = await f._fetch_all_columns_in_schema("DB", "S")
        assert result == {"T": [{"name": "col"}]}
        f.data_source.execute_sql.assert_not_called()

    async def test_failure_returns_empty(self):
        f = _make_fetcher()
        f.data_source.execute_sql.return_value = _response(False, error="x")
        assert await f._fetch_all_columns_in_schema("DB", "S") == {}

    async def test_parses_columns(self):
        f = _make_fetcher()
        data = {
            "resultSetMetaData": {"rowType": [
                {"name": "TABLE_NAME"}, {"name": "COLUMN_NAME"}, {"name": "DATA_TYPE"},
                {"name": "CHARACTER_MAXIMUM_LENGTH"}, {"name": "NUMERIC_PRECISION"},
                {"name": "NUMERIC_SCALE"}, {"name": "IS_NULLABLE"},
                {"name": "COLUMN_DEFAULT"}, {"name": "COMMENT"},
            ]},
            "data": [
                ["T", "id", "NUMBER", None, 38, 0, "NO", None, "pk"],
                ["T", "name", "VARCHAR", 100, None, None, "YES", None, None],
            ],
        }
        f.data_source.execute_sql.return_value = _response(True, data)
        result = await f._fetch_all_columns_in_schema("DB", "S")
        assert "T" in result
        assert len(result["T"]) == 2
        assert result["T"][0]["nullable"] is False
        assert result["T"][1]["nullable"] is True
        # Cache populated
        assert "DB.S" in f._columns_cache


@pytest.mark.asyncio
class TestGetTableDdl:
    async def test_no_warehouse(self):
        f = _make_fetcher(warehouse=None)
        assert await f.get_table_ddl("DB", "S", "T") is None

    async def test_failure_returns_none(self):
        f = _make_fetcher()
        f.data_source.execute_sql.return_value = _response(False, error="e")
        assert await f.get_table_ddl("DB", "S", "T") is None

    async def test_success_returns_ddl(self):
        f = _make_fetcher()
        data = {
            "resultSetMetaData": {"rowType": [{"name": "DDL"}]},
            "data": [["CREATE TABLE T (id INT)"]],
        }
        f.data_source.execute_sql.return_value = _response(True, data)
        ddl = await f.get_table_ddl("DB", "S", "T")
        assert ddl == "CREATE TABLE T (id INT)"

    async def test_no_rows_returns_none(self):
        f = _make_fetcher()
        data = {"resultSetMetaData": {"rowType": [{"name": "DDL"}]}, "data": []}
        f.data_source.execute_sql.return_value = _response(True, data)
        assert await f.get_table_ddl("DB", "S", "T") is None


@pytest.mark.asyncio
class TestFetchForeignKeys:
    async def test_no_warehouse(self):
        f = _make_fetcher(warehouse=None)
        assert await f._fetch_foreign_keys_in_schema("DB", "S") == []

    async def test_failure_returns_empty(self):
        f = _make_fetcher()
        f.data_source.execute_sql.return_value = _response(False, error="e")
        assert await f._fetch_foreign_keys_in_schema("DB", "S") == []

    async def test_parses_lowercase_fields(self):
        f = _make_fetcher()
        data = {
            "resultSetMetaData": {"rowType": [
                {"name": "fk_schema_name"}, {"name": "fk_table_name"}, {"name": "fk_column_name"},
                {"name": "pk_schema_name"}, {"name": "pk_table_name"}, {"name": "pk_column_name"},
                {"name": "fk_name"},
            ]},
            "data": [["S1", "T1", "c1", "S2", "T2", "c2", "fk1"]],
        }
        f.data_source.execute_sql.return_value = _response(True, data)
        fks = await f._fetch_foreign_keys_in_schema("DB", "S1")
        assert len(fks) == 1
        assert fks[0].constraint_name == "fk1"
        assert fks[0].source_table == "T1"
        assert fks[0].target_table == "T2"

    async def test_skips_rows_missing_tables(self):
        f = _make_fetcher()
        data = {
            "resultSetMetaData": {"rowType": [
                {"name": "FK_TABLE_NAME"}, {"name": "PK_TABLE_NAME"},
            ]},
            "data": [[None, "T2"], ["T1", None]],
        }
        f.data_source.execute_sql.return_value = _response(True, data)
        fks = await f._fetch_foreign_keys_in_schema("DB", "S")
        assert fks == []


@pytest.mark.asyncio
class TestFetchPrimaryKeys:
    async def test_no_warehouse(self):
        f = _make_fetcher(warehouse=None)
        assert await f._fetch_primary_keys_in_schema("DB", "S") == []

    async def test_failure_returns_empty(self):
        f = _make_fetcher()
        f.data_source.execute_sql.return_value = _response(False, error="e")
        assert await f._fetch_primary_keys_in_schema("DB", "S") == []

    async def test_parses_rows(self):
        f = _make_fetcher()
        data = {
            "resultSetMetaData": {"rowType": [
                {"name": "table_name"}, {"name": "column_name"},
            ]},
            "data": [["T", "id"], ["T", "name"], [None, "x"]],
        }
        f.data_source.execute_sql.return_value = _response(True, data)
        pks = await f._fetch_primary_keys_in_schema("DB", "S")
        assert len(pks) == 2
        assert pks[0] == {"table": "T", "column": "id"}


# ===========================================================================
# fetch_all integration
# ===========================================================================


@pytest.mark.asyncio
class TestFetchAll:
    async def test_end_to_end(self, tmp_path):
        f = _make_fetcher()
        f.data_source.list_databases.return_value = _response(
            True, [{"name": "DB"}, {"name": "OTHER"}]
        )
        f.data_source.list_schemas.return_value = _response(True, [{"name": "S"}])
        f.data_source.list_tables.return_value = _response(
            True, [{"name": "T"}]
        )
        f.data_source.list_views.return_value = _response(True, [{"name": "V"}])
        f.data_source.list_stages.return_value = _response(True, [{"name": "St"}])
        f.data_source.list_stage_files.return_value = _response(
            True, {"data": [["file.csv", "10", "", "md5"]]}
        )

        # columns + FKs + PKs via execute_sql
        columns_data = {
            "resultSetMetaData": {"rowType": [
                {"name": "TABLE_NAME"}, {"name": "COLUMN_NAME"}, {"name": "DATA_TYPE"},
                {"name": "CHARACTER_MAXIMUM_LENGTH"}, {"name": "NUMERIC_PRECISION"},
                {"name": "NUMERIC_SCALE"}, {"name": "IS_NULLABLE"},
                {"name": "COLUMN_DEFAULT"}, {"name": "COMMENT"},
            ]},
            "data": [["T", "id", "NUMBER", None, 38, 0, "NO", None, ""]],
        }
        fks_data = {
            "resultSetMetaData": {"rowType": [
                {"name": "fk_schema_name"}, {"name": "fk_table_name"},
                {"name": "fk_column_name"}, {"name": "pk_schema_name"},
                {"name": "pk_table_name"}, {"name": "pk_column_name"},
                {"name": "fk_name"},
            ]},
            "data": [["S", "T", "id", "S", "T2", "id", "fk1"]],
        }
        pks_data = {
            "resultSetMetaData": {"rowType": [
                {"name": "table_name"}, {"name": "column_name"},
            ]},
            "data": [["T", "id"]],
        }

        async def execute_sql_side_effect(statement: str, **kwargs):
            if "INFORMATION_SCHEMA.COLUMNS" in statement:
                return _response(True, columns_data)
            if "SHOW IMPORTED KEYS" in statement:
                return _response(True, fks_data)
            if "SHOW PRIMARY KEYS" in statement:
                return _response(True, pks_data)
            return _response(True, {})

        f.data_source.execute_sql.side_effect = execute_sql_side_effect

        hierarchy = await f.fetch_all(
            database_filter=["DB"],
            schema_filter=["DB.S"],
            include_files=True,
            include_relationships=True,
        )
        assert len(hierarchy.databases) == 1
        assert hierarchy.databases[0].name == "DB"
        assert hierarchy.tables["DB.S"][0].columns[0]["name"] == "id"
        assert hierarchy.tables["DB.S"][0].foreign_keys[0]["column"] == "id"
        assert hierarchy.tables["DB.S"][0].primary_keys == ["id"]
        assert len(hierarchy.foreign_keys) == 1
        assert hierarchy.fetched_at  # set

        # Test save_to_file path
        out = tmp_path / "nested" / "out.json"
        f.save_to_file(str(out))
        assert out.exists()
        content = json.loads(out.read_text())
        assert content["summary"]["databases"] == 1

    async def test_skip_files_and_relationships(self):
        f = _make_fetcher()
        f.data_source.list_databases.return_value = _response(True, [{"name": "DB"}])
        f.data_source.list_schemas.return_value = _response(True, [{"name": "S"}])
        f.data_source.list_tables.return_value = _response(True, [{"name": "T"}])
        f.data_source.list_views.return_value = _response(True, [])
        f.data_source.list_stages.return_value = _response(True, [{"name": "St"}])
        f.data_source.list_stage_files.return_value = _response(True, {"data": []})

        h = await f.fetch_all(include_files=False, include_relationships=False)
        assert h.foreign_keys == []
        # Still populated for stages but no files fetched
        f.data_source.list_stage_files.assert_not_called()

    async def test_schema_filter_empty_when_no_match(self):
        f = _make_fetcher()
        f.data_source.list_databases.return_value = _response(True, [{"name": "DB"}])
        f.data_source.list_schemas.return_value = _response(True, [{"name": "S"}])
        f.data_source.list_tables.return_value = _response(True, [])
        f.data_source.list_views.return_value = _response(True, [])
        f.data_source.list_stages.return_value = _response(True, [])

        h = await f.fetch_all(schema_filter=["DB.OTHER"])
        assert h.schemas["DB"] == []


# ===========================================================================
# print_summary
# ===========================================================================


class TestPrintSummary:
    def test_prints(self, capsys):
        f = _make_fetcher()
        f.print_summary()
        out = capsys.readouterr().out
        assert "Snowflake Data Summary" in out
        assert "databases: 0" in out
