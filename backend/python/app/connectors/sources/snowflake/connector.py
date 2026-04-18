"""
Snowflake Connector

Syncs databases, namespaces, tables, views, stages and files from Snowflake.
"""
import asyncio
import hashlib
import json
import mimetypes
import os
import re
import uuid
from collections.abc import AsyncGenerator
from dataclasses import dataclass
from logging import Logger
from typing import Any, Optional

from aiolimiter import AsyncLimiter
from fastapi import HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from app.config.configuration_service import ConfigurationService
from app.config.constants.arangodb import (
    Connectors,
    MimeTypes,
    OriginTypes,
    RecordRelations,
)
from app.connectors.core.base.connector.connector_service import BaseConnector
from app.connectors.core.base.data_processor.data_source_entities_processor import (
    DataSourceEntitiesProcessor,
)
from app.connectors.core.base.data_store.data_store import DataStoreProvider
from app.connectors.core.base.sync_point.sync_point import (
    SyncDataPointType,
    SyncPoint,
)
from app.connectors.core.constants import OAuthDefaults
from app.connectors.core.registry.auth_builder import (
    AuthBuilder,
    AuthType,
    OAuthScopeConfig,
)
from app.connectors.core.registry.connector_builder import (
    AuthField,
    CommonFields,
    ConnectorBuilder,
    ConnectorScope,
    DocumentationLink,
    SyncStrategy,
)
from app.connectors.core.registry.types import FieldType
from app.connectors.core.registry.filters import (
    FilterCategory,
    FilterCollection,
    FilterField,
    FilterOption,
    FilterOptionsResponse,
    FilterType,
    IndexingFilterKey,
    MultiselectOperator,
    NumberOperator,
    OptionSourceType,
    load_connector_filters,
)
from app.connectors.sources.snowflake.apps import SnowflakeApp
from app.connectors.sources.snowflake.data_fetcher import (
    SnowflakeDatabase,
    SnowflakeDataFetcher,
    SnowflakeFile,
    SnowflakeSchema,
    SnowflakeStage,
    SnowflakeTable,
    SnowflakeView,
    quote_fqn,
)
from app.models.entities import (
    AppUser,
    FileRecord,
    ProgressStatus,
    Record,
    RecordGroup,
    RecordGroupType,
    RecordType,
    RelatedExternalRecord,
    SQLTableRecord,
    SQLViewRecord,
    User,
)
from app.models.permission import EntityType, Permission, PermissionType
from app.sources.client.snowflake.snowflake import (
    SnowflakeClient,
    SnowflakeOAuthConfig,
    SnowflakePATConfig,
)
from app.sources.external.snowflake.snowflake_ import SnowflakeDataSource
from app.utils.streaming import create_stream_record_response
from app.utils.time_conversion import get_epoch_timestamp_in_ms

MAX_ROWS_PER_TABLE_LIMIT = 10000

# Snowflake OAuth endpoints are per-account. Users provide the concrete URLs
# via the `authorizeUrl` / `tokenUrl` auth fields at OAuth configuration time
# (same pattern as ServiceNow). The values below are examples shown in the
# registry; the real URLs come from user input and override these at runtime.
# See: https://docs.snowflake.com/en/user-guide/oauth-custom
SNOWFLAKE_OAUTH_AUTHORIZE_URL_EXAMPLE = "https://<YOUR_ACCOUNT_IDENTIFIER>.snowflakecomputing.com/oauth/authorize"
SNOWFLAKE_OAUTH_TOKEN_URL_EXAMPLE = "https://<YOUR_ACCOUNT_IDENTIFIER>.snowflakecomputing.com/oauth/token-request"


def get_file_extension(path: str) -> Optional[str]:
    if "." in path:
        parts = path.split(".")
        if len(parts) > 1:
            return parts[-1].lower()
    return None


def get_mimetype_from_path(path: str, is_folder: bool = False) -> str:
    if is_folder:
        return MimeTypes.FOLDER.value
    mime_type, _ = mimetypes.guess_type(path)
    if mime_type:
        try:
            return MimeTypes(mime_type).value
        except ValueError:
            return MimeTypes.BIN.value
    return MimeTypes.BIN.value


class SnowflakeTableState(BaseModel):
    column_signature: str = ""
    last_altered: Optional[str] = None
    row_count: Optional[int] = None
    bytes: Optional[int] = None


class SnowflakeViewState(BaseModel):
    definition_hash: str = ""


class SnowflakeFileState(BaseModel):
    md5: Optional[str] = None
    size: Optional[int] = None
    last_modified: Optional[str] = None


@dataclass
class SyncStats:
    databases_synced: int = 0
    schemas_synced: int = 0
    stages_synced: int = 0
    tables_new: int = 0
    tables_updated: int = 0
    tables_deleted: int = 0
    views_new: int = 0
    views_updated: int = 0
    views_deleted: int = 0
    files_new: int = 0
    files_updated: int = 0
    files_deleted: int = 0
    errors: int = 0

    def to_dict(self) -> dict[str, int]:
        return {
            'databases_synced': self.databases_synced,
            'schemas_synced': self.schemas_synced,
            'stages_synced': self.stages_synced,
            'tables_new': self.tables_new,
            'tables_updated': self.tables_updated,
            'tables_deleted': self.tables_deleted,
            'views_new': self.views_new,
            'views_updated': self.views_updated,
            'views_deleted': self.views_deleted,
            'files_new': self.files_new,
            'files_updated': self.files_updated,
            'files_deleted': self.files_deleted,
            'errors': self.errors,
        }

    def log_summary(self, logger) -> None:
        logger.info(
            f"📊 Sync Stats: "
            f"DBs={self.databases_synced}, Schemas={self.schemas_synced}, Stages={self.stages_synced} | "
            f"Tables(new={self.tables_new}, updated={self.tables_updated}, deleted={self.tables_deleted}) | "
            f"Views(new={self.views_new}, updated={self.views_updated}, deleted={self.views_deleted}) | "
            f"Files(new={self.files_new}, updated={self.files_updated}, deleted={self.files_deleted}) | "
            f"Errors={self.errors}"
        )


@ConnectorBuilder("Snowflake")\
    .in_group("Snowflake")\
    .with_description("Sync databases, tables, views, stages and files from Snowflake")\
    .with_categories(["Database", "Data Warehouse"])\
    .with_scopes([ConnectorScope.PERSONAL.value, ConnectorScope.TEAM.value])\
    .with_auth([
        AuthBuilder.type(AuthType.API_TOKEN).fields([
            AuthField(
                name="accountIdentifier",
                display_name="Account Identifier",
                placeholder="abc12345.us-east-1",
                description="Snowflake account identifier (e.g., abc12345.us-east-1)",
                field_type="TEXT",
                max_length=500,
                is_secret=False,
                required=True,
            ),
            AuthField(
                name="patToken",
                display_name="Personal Access Token",
                placeholder="Enter your PAT token",
                description="Personal Access Token from Snowflake",
                field_type="PASSWORD",
                max_length=2000,
                is_secret=True,
                required=True,
            ),
            AuthField(
                name="warehouse",
                display_name="Warehouse",
                placeholder="COMPUTE_WH",
                description="Default warehouse for query execution",
                field_type="TEXT",
                max_length=200,
                is_secret=False,
                required=True,
            ),
        ]),
        AuthBuilder.type(AuthType.OAUTH).oauth(
            connector_name="Snowflake",
            authorize_url=SNOWFLAKE_OAUTH_AUTHORIZE_URL_EXAMPLE,
            token_url=SNOWFLAKE_OAUTH_TOKEN_URL_EXAMPLE,
            redirect_uri="connectors/oauth/callback/Snowflake",
            scopes=OAuthScopeConfig(
                personal_sync=["session:role:PUBLIC"],
                team_sync=["session:role:PUBLIC"],
                agent=[],
            ),
            fields=[
                AuthField(
                    name="accountIdentifier",
                    display_name="Account Identifier",
                    placeholder="abc12345.us-east-1",
                    description="Snowflake account identifier (e.g., abc12345.us-east-1). Used for the SQL API host.",
                    field_type=FieldType.TEXT.value,
                    max_length=500,
                    is_secret=False,
                    required=True,
                ),
                AuthField(
                    name="authorizeUrl",
                    display_name="Snowflake Authorize URL",
                    placeholder="https://abc12345.us-east-1.snowflakecomputing.com/oauth/authorize",
                    description="Your Snowflake OAuth authorize URL (e.g., {SNOWFLAKE_OAUTH_AUTHORIZE_URL_EXAMPLE}).",
                    field_type=FieldType.URL.value,
                    max_length=OAuthDefaults.MAX_URL_LENGTH,
                    is_secret=False,
                    required=True,
                ),
                AuthField(
                    name="tokenUrl",
                    display_name="Snowflake Token URL",
                    placeholder="https://abc12345.us-east-1.snowflakecomputing.com/oauth/token-request",
                    description="Your Snowflake OAuth token URL (e.g., {SNOWFLAKE_OAUTH_TOKEN_URL_EXAMPLE}).",
                    field_type=FieldType.URL.value,
                    max_length=OAuthDefaults.MAX_URL_LENGTH,
                    is_secret=False,
                    required=True,
                ),
                CommonFields.client_id("Snowflake Security Integration"),
                CommonFields.client_secret("Snowflake Security Integration"),
                AuthField(
                    name="warehouse",
                    display_name="Warehouse",
                    placeholder="COMPUTE_WH",
                    description="Default warehouse for query execution",
                    field_type=FieldType.TEXT.value,
                    max_length=200,
                    is_secret=False,
                    required=True,
                ),
            ],
            icon_path="/assets/icons/connectors/snowflake.svg",
            app_group="Snowflake",
            app_description="OAuth application for accessing Snowflake SQL API",
            app_categories=["Database", "Data Warehouse"],
            additional_params={
                "response_type": "code",
            },
        ),
    ])\
    .configure(lambda builder: builder
        .with_icon("/assets/icons/connectors/snowflake.svg")
        .add_documentation_link(DocumentationLink(
            "Snowflake PAT Setup",
            "https://docs.snowflake.com/en/developer-guide/sql-api/authenticating",
            "setup"
        ))
        .add_filter_field(FilterField(
            name="databases",
            display_name="Databases",
            filter_type=FilterType.MULTISELECT,
            category=FilterCategory.SYNC,
            description="Select specific databases to sync",
            option_source_type=OptionSourceType.DYNAMIC,
            default_operator=MultiselectOperator.IN.value,
        ))
        .add_filter_field(FilterField(
            name="schemas",
            display_name="Schemas",
            filter_type=FilterType.MULTISELECT,
            category=FilterCategory.SYNC,
            description="Select specific schemas to sync (Format: Database.Schema)",
            option_source_type=OptionSourceType.DYNAMIC,
            default_operator=MultiselectOperator.IN.value,
        ))
        .add_filter_field(FilterField(
            name="tables",
            display_name="Tables",
            filter_type=FilterType.MULTISELECT,
            category=FilterCategory.SYNC,
            description="Select specific tables to sync",
            option_source_type=OptionSourceType.DYNAMIC,
            default_operator=MultiselectOperator.IN.value,
        ))
        .add_filter_field(FilterField(
            name="views",
            display_name="Views",
            filter_type=FilterType.MULTISELECT,
            category=FilterCategory.SYNC,
            description="Select specific views to sync",
            option_source_type=OptionSourceType.DYNAMIC,
            default_operator=MultiselectOperator.IN.value,
        ))
        .add_filter_field(FilterField(
            name="stages",
            display_name="Stages",
            filter_type=FilterType.MULTISELECT,
            category=FilterCategory.SYNC,
            description="Select specific stages to sync",
            option_source_type=OptionSourceType.DYNAMIC,
            default_operator=MultiselectOperator.IN.value,
        ))
        .add_filter_field(FilterField(
            name="files",
            display_name="Files",
            filter_type=FilterType.MULTISELECT,
            category=FilterCategory.SYNC,
            description="Select specific files to sync (Format: Database.Schema.Stage/path/to/file)",
            option_source_type=OptionSourceType.DYNAMIC,
            default_operator=MultiselectOperator.IN.value,
        ))
        .add_filter_field(FilterField(
            name=IndexingFilterKey.TABLES.value,
            display_name="Index Tables",
            filter_type=FilterType.BOOLEAN,
            category=FilterCategory.INDEXING,
            description="Enable indexing of tables",
            default_value=True,
        ))
        .add_filter_field(FilterField(
            name=IndexingFilterKey.VIEWS.value,
            display_name="Index Views",
            filter_type=FilterType.BOOLEAN,
            category=FilterCategory.INDEXING,
            description="Enable indexing of views",
            default_value=True,
        ))
        .add_filter_field(FilterField(
            name=IndexingFilterKey.STAGE_FILES.value,
            display_name="Index Stage Files",
            filter_type=FilterType.BOOLEAN,
            category=FilterCategory.INDEXING,
            description="Enable indexing of files in stages",
            default_value=True,
        ))
        .add_filter_field(FilterField(
            name=IndexingFilterKey.MAX_ROWS_PER_TABLE.value,
            display_name="Max Rows Per Table",
            filter_type=FilterType.NUMBER,
            category=FilterCategory.SYNC,
            description="Maximum number of rows to stream per table (max 10000)",
            default_value=1000,
            default_operator=NumberOperator.LESS_THAN_OR_EQUAL.value,
        ))
        .add_filter_field(CommonFields.enable_manual_sync_filter())
        .with_sync_strategies([SyncStrategy.SCHEDULED, SyncStrategy.MANUAL])
        .with_scheduled_config(True, 120)
        .with_sync_support(True)
        .with_agent_support(False)
    )\
    .build_decorator()
class SnowflakeConnector(BaseConnector):

    def __init__(
        self,
        logger: Logger,
        data_entities_processor: DataSourceEntitiesProcessor,
        data_store_provider: DataStoreProvider,
        config_service: ConfigurationService,
        connector_id: str,
        scope: str = ConnectorScope.TEAM.value,
        created_by: Optional[str] = None,
    ) -> None:
        super().__init__(
            SnowflakeApp(connector_id),
            logger,
            data_entities_processor,
            data_store_provider,
            config_service,
            connector_id,
            scope,
            created_by,
        )
        self.connector_id = connector_id
        self.connector_name = Connectors.SNOWFLAKE
        self.data_source: Optional[SnowflakeDataSource] = None
        self.data_fetcher: Optional[SnowflakeDataFetcher] = None
        self.warehouse: Optional[str] = None
        self.account_identifier: Optional[str] = None
        self.batch_size = 100
        self.rate_limiter = AsyncLimiter(25, 1)
        self.connector_scope: Optional[str] = None
        self.created_by: Optional[str] = None
        self.sync_filters: FilterCollection = FilterCollection()
        self.indexing_filters: FilterCollection = FilterCollection()
        self._record_id_cache: dict[str, str] = {}
        self.sync_stats: SyncStats = SyncStats()

        self._hierarchy_cache = None
        self._filter_cache_rebuild_event: Optional[asyncio.Event] = None

        org_id = self.data_entities_processor.org_id
        self.sync_point = SyncPoint(
            connector_id=self.connector_id,
            org_id=org_id,
            sync_data_point_type=SyncDataPointType.RECORDS,
            data_store_provider=data_store_provider,
        )

    def get_app_users(self, users: list[User]) -> list[AppUser]:
        return [
            AppUser(
                app_name=self.connector_name,
                connector_id=self.connector_id,
                source_user_id=user.source_user_id or user.id or user.email,
                org_id=user.org_id or self.data_entities_processor.org_id,
                email=user.email,
                full_name=user.full_name or user.email,
                is_active=user.is_active if user.is_active is not None else True,
                title=user.title,
            )
            for user in users
            if user.email
        ]

    async def _create_app_users(self) -> None:
        try:
            all_active_users = await self.data_entities_processor.get_all_active_users()
            app_users = self.get_app_users(all_active_users)
            await self.data_entities_processor.on_new_app_users(app_users)
            self.logger.info(f"Created {len(app_users)} app users for Snowflake connector")
        except Exception as e:
            self.logger.error(f"Error creating app users: {e}", exc_info=True)
            raise

    async def _ensure_scope_app_edges(self) -> None:
        """Ensure connector-app edges exist for TEAM/PERSONAL scopes before sync."""
        if self.scope == ConnectorScope.TEAM.value:
            async with self.data_store_provider.transaction() as tx_store:
                await tx_store.ensure_team_app_edge(
                    self.connector_id,
                    self.data_entities_processor.org_id,
                )
            return

        # Personal: create user-app edge only for the creator
        if self.created_by:
            creator_user = await self.data_entities_processor.get_user_by_user_id(self.created_by)
            if creator_user and getattr(creator_user, "email", None):
                app_users = self.get_app_users([creator_user])
                await self.data_entities_processor.on_new_app_users(app_users)
            else:
                self.logger.warning(
                    "Creator user not found or has no email for created_by %s; skipping user-app edges.",
                    self.created_by,
                )
        else:
            self.logger.warning(
                "Personal connector has no created_by; skipping user-app edges."
            )

    async def init(self) -> bool:
        try:
            config = await self.config_service.get_config(
                f"/services/connectors/{self.connector_id}/config"
            )
            if not config:
                self.logger.error("Snowflake configuration not found")
                return False

            auth_config = config.get("auth") or {}
            credentials_config = config.get("credentials") or {}

            self.account_identifier = auth_config.get("accountIdentifier")
            self.warehouse = auth_config.get("warehouse")

            if not self.account_identifier:
                self.logger.error("Missing accountIdentifier in configuration")
                return False

            self.scope = config.get("scope", self.scope or ConnectorScope.TEAM.value)
            self.connector_scope = self.scope
            self.created_by = config.get("created_by", self.created_by)

            auth_type = auth_config.get("authType", "API_TOKEN")

            client: Optional[SnowflakeClient] = None

            if auth_type == "OAUTH":
                oauth_token = credentials_config.get("access_token")
                if not oauth_token:
                    self.logger.error("OAuth access token missing in credentials")
                    return False
                self.logger.info("Using OAuth authentication for Snowflake")
                oauth_config = SnowflakeOAuthConfig(
                    account_identifier=self.account_identifier,
                    oauth_token=oauth_token,
                )
                client = SnowflakeClient(oauth_config.create_client())
            elif auth_type == "API_TOKEN":
                pat_token = auth_config.get("patToken")
                if not pat_token:
                    self.logger.error("PAT token missing in auth configuration")
                    return False
                self.logger.info("Using PAT authentication for Snowflake")
                pat_config = SnowflakePATConfig(
                    account_identifier=self.account_identifier,
                    pat_token=pat_token,
                )
                client = SnowflakeClient(pat_config.create_client())
            else:
                self.logger.error(f"Unsupported auth type for Snowflake: {auth_type}")
                return False

            self.data_source = SnowflakeDataSource(client)
            self.data_fetcher = SnowflakeDataFetcher(self.data_source, self.warehouse)

            self.logger.info("Snowflake connector initialized successfully")
            return True

        except Exception as e:
            self.logger.error(f"Failed to initialize Snowflake connector: {e}", exc_info=True)
            return False

    async def run_sync(self) -> None:
        try:
            self.logger.info("📦 [Sync] Starting Snowflake sync...")

            if not self.data_fetcher:
                raise ConnectionError("Snowflake connector not initialized")

            self.sync_filters, self.indexing_filters = await load_connector_filters(
                self.config_service, "snowflake", self.connector_id, self.logger
            )

            await self._ensure_scope_app_edges()

            self.sync_stats = SyncStats()

            sync_point_key = "snowflake_sync_state"
            stored_state = await self.sync_point.read_sync_point(sync_point_key)

            if stored_state and stored_state.get("table_states"):
                self.logger.info("📦 [Sync] Found existing sync state, running incremental sync...")
                await self.run_incremental_sync()
            else:
                self.logger.info("📦 [Sync] No existing sync state, running full sync...")
                await self._run_full_sync_internal()

            self.sync_stats.log_summary(self.logger)

        except Exception as e:
            self.logger.error(f"❌ [Sync] Error: {e}", exc_info=True)
            raise

    def _get_filter_values(self) -> tuple[
        Optional[list[str]], Optional[list[str]], Optional[list[str]],
        Optional[list[str]], Optional[list[str]], Optional[list[str]]
    ]:
        db_filter = self.sync_filters.get("databases")
        selected_dbs = db_filter.value if db_filter and db_filter.value else None

        schema_filter = self.sync_filters.get("schemas")
        selected_schemas = schema_filter.value if schema_filter and schema_filter.value else None

        table_filter = self.sync_filters.get("tables")
        selected_tables = table_filter.value if table_filter and table_filter.value else None

        view_filter = self.sync_filters.get("views")
        selected_views = view_filter.value if view_filter and view_filter.value else None

        stage_filter = self.sync_filters.get("stages")
        selected_stages = stage_filter.value if stage_filter and stage_filter.value else None

        file_filter = self.sync_filters.get("files")
        selected_files = file_filter.value if file_filter and file_filter.value else None

        return selected_dbs, selected_schemas, selected_tables, selected_views, selected_stages, selected_files

    def _compute_effective_filters(self) -> tuple[
        Optional[list[str]],  # effective_dbs: pass to fetch_all
        Optional[list[str]],  # effective_schemas: pass to fetch_all (DB.SCHEMA)
        Optional[list[str]],  # effective_stages: per-schema stage filter (fqn)
        Optional[list[str]],  # selected_tables (fqn)
        Optional[list[str]],  # selected_views (fqn)
        Optional[list[str]],  # selected_files (DB.SCHEMA.STAGE/path)
    ]:
        # Derive implied parent DBs/schemas/stages from leaf-level filters so a
        # tables/views/stages/files selection doesn't leak sync of unrelated parents.
        (selected_dbs, selected_schemas, selected_tables,
         selected_views, selected_stages, selected_files) = self._get_filter_values()

        implied_dbs: set[str] = set()
        implied_schemas: set[str] = set()
        implied_stages: set[str] = set()

        def _add_parents_from_fqn(fqn: str) -> None:
            parts = fqn.split(".")
            if len(parts) >= 1 and parts[0]:
                implied_dbs.add(parts[0])
            if len(parts) >= 2:
                implied_schemas.add(f"{parts[0]}.{parts[1]}")

        for fqn in (selected_schemas or []):
            parts = fqn.split(".")
            if parts and parts[0]:
                implied_dbs.add(parts[0])
        for fqn in (selected_tables or []):
            _add_parents_from_fqn(fqn)
        for fqn in (selected_views or []):
            _add_parents_from_fqn(fqn)
        for fqn in (selected_stages or []):
            _add_parents_from_fqn(fqn)
        for file_id in (selected_files or []):
            stage_fqn = file_id.split("/", 1)[0]
            _add_parents_from_fqn(stage_fqn)
            implied_stages.add(stage_fqn)

        effective_dbs = (
            sorted(set(selected_dbs or []) | implied_dbs)
            if (selected_dbs or implied_dbs) else None
        )
        effective_schemas = (
            sorted(set(selected_schemas or []) | implied_schemas)
            if (selected_schemas or implied_schemas) else None
        )
        effective_stages = (
            sorted(set(selected_stages or []) | implied_stages)
            if (selected_stages or implied_stages) else None
        )

        return (
            effective_dbs,
            effective_schemas,
            effective_stages,
            selected_tables,
            selected_views,
            selected_files,
        )

    async def _run_full_sync_internal(self) -> None:
        try:
            self.logger.info("📦 [Full Sync] Starting full sync...")
            self._record_id_cache.clear()

            await self._create_app_users()

            (effective_dbs, effective_schemas, effective_stages,
             selected_tables, selected_views, selected_files) = self._compute_effective_filters()

            hierarchy = await self.data_fetcher.fetch_all(
                database_filter=effective_dbs,
                schema_filter=effective_schemas,
                include_files=True,
                include_relationships=True,
            )
            self.logger.info(f"Fetched hierarchy: {hierarchy.summary()}")

            await self._sync_databases(hierarchy.databases)
            self.sync_stats.databases_synced = len(hierarchy.databases)

            for db in hierarchy.databases:
                schemas = hierarchy.schemas.get(db.name, [])

                await self._sync_namespaces(db.name, schemas)
                self.sync_stats.schemas_synced += len(schemas)

                for schema in schemas:
                    schema_key = f"{db.name}.{schema.name}"
                    tables = hierarchy.tables.get(schema_key, [])
                    views = hierarchy.views.get(schema_key, [])
                    stages = hierarchy.stages.get(schema_key, [])

                    if selected_tables:
                        tables = [t for t in tables if t.fqn in selected_tables]
                    if selected_views:
                        views = [v for v in views if v.fqn in selected_views]
                    if effective_stages:
                        stages = [s for s in stages if s.fqn in effective_stages]

                    await self._sync_tables(db.name, schema.name, tables)
                    self.sync_stats.tables_new += len(tables)

                    await self._sync_views(db.name, schema.name, views)
                    self.sync_stats.views_new += len(views)

                    await self._sync_stages(db.name, schema.name, stages)
                    self.sync_stats.stages_synced += len(stages)

                    for stage in stages:
                        stage_key = f"{db.name}.{schema.name}.{stage.name}"
                        files = hierarchy.files.get(stage_key, [])

                        if selected_files:
                            files = [
                                f for f in files
                                if f"{stage_key}/{f.relative_path}" in selected_files
                            ]

                        await self._sync_stage_files(db.name, schema.name, stage.name, files)
                        self.sync_stats.files_new += len([f for f in files if not f.is_folder])

            await self._save_sync_state("snowflake_sync_state", hierarchy)

            self.logger.info("✅ [Full Sync] Snowflake full sync completed")
        except Exception as e:
            self.sync_stats.errors += 1
            self.logger.error(f"❌ [Full Sync] Error: {e}", exc_info=True)
            raise

    async def run_incremental_sync(self) -> None:
        """
        Run incremental sync for Snowflake.

        Change detection:
        - Tables: last_altered timestamp + column signature
        - Views: MD5 hash of GET_DDL definition
        - Files: last_modified + md5
        - Deleted objects: present in stored state but absent from current fetch
        """
        self.logger.info("📦 [Incremental Sync] Starting Snowflake incremental sync...")

        if not self.data_fetcher:
            raise ConnectionError("Snowflake connector not initialized")

        self.sync_filters, self.indexing_filters = await load_connector_filters(
            self.config_service, "snowflake", self.connector_id, self.logger
        )

        await self._ensure_scope_app_edges()

        try:
            sync_point_key = "snowflake_sync_state"
            stored_state = await self.sync_point.read_sync_point(sync_point_key)

            if not stored_state or not stored_state.get("table_states"):
                self.logger.info("No previous sync state found, running full sync")
                await self._run_full_sync_internal()
                return

            stored_table_states: dict[str, SnowflakeTableState] = {
                fqn: SnowflakeTableState.model_validate(state)
                for fqn, state in json.loads(stored_state.get("table_states", "{}")).items()
            }
            stored_view_states: dict[str, SnowflakeViewState] = {
                fqn: SnowflakeViewState.model_validate(state)
                for fqn, state in json.loads(stored_state.get("view_states", "{}")).items()
            }
            stored_file_states: dict[str, SnowflakeFileState] = {
                file_id: SnowflakeFileState.model_validate(state)
                for file_id, state in json.loads(stored_state.get("file_states", "{}")).items()
            }

            (effective_dbs, effective_schemas, effective_stages,
             selected_tables, selected_views, selected_files) = self._compute_effective_filters()

            hierarchy = await self.data_fetcher.fetch_all(
                database_filter=effective_dbs,
                schema_filter=effective_schemas,
                include_files=True,
                include_relationships=True,
            )
            self.logger.info(f"Fetched hierarchy: {hierarchy.summary()}")

            # Always sync databases / namespaces / stages (small, and needed as parents for new items)
            await self._sync_databases(hierarchy.databases)
            self.sync_stats.databases_synced = len(hierarchy.databases)

            current_tables: dict[str, tuple[str, str, SnowflakeTable]] = {}
            current_views: dict[str, tuple[str, str, SnowflakeView]] = {}
            current_files: dict[str, tuple[str, str, str, SnowflakeFile]] = {}

            for db in hierarchy.databases:
                schemas = hierarchy.schemas.get(db.name, [])

                await self._sync_namespaces(db.name, schemas)
                self.sync_stats.schemas_synced += len(schemas)

                for schema in schemas:
                    schema_key = f"{db.name}.{schema.name}"
                    tables = hierarchy.tables.get(schema_key, [])
                    views = hierarchy.views.get(schema_key, [])
                    stages = hierarchy.stages.get(schema_key, [])

                    if selected_tables:
                        tables = [t for t in tables if t.fqn in selected_tables]
                    if selected_views:
                        views = [v for v in views if v.fqn in selected_views]
                    if effective_stages:
                        stages = [s for s in stages if s.fqn in effective_stages]

                    await self._sync_stages(db.name, schema.name, stages)
                    self.sync_stats.stages_synced += len(stages)

                    for table in tables:
                        current_tables[table.fqn] = (db.name, schema.name, table)

                    for view in views:
                        current_views[view.fqn] = (db.name, schema.name, view)

                    for stage in stages:
                        stage_key = f"{db.name}.{schema.name}.{stage.name}"
                        files = hierarchy.files.get(stage_key, [])
                        if selected_files:
                            files = [
                                f for f in files
                                if f"{stage_key}/{f.relative_path}" in selected_files
                            ]
                        for file in files:
                            if file.is_folder:
                                continue
                            file_id = f"{stage_key}/{file.relative_path}"
                            current_files[file_id] = (db.name, schema.name, stage.name, file)

            # Table change detection
            new_tables = [t for fqn, t in current_tables.items() if fqn not in stored_table_states]
            changed_tables = [
                t for fqn, t in current_tables.items()
                if fqn in stored_table_states
                and self._has_table_changed(t[2], stored_table_states[fqn])
            ]
            deleted_table_fqns = [fqn for fqn in stored_table_states if fqn not in current_tables]

            # View change detection
            new_views = [v for fqn, v in current_views.items() if fqn not in stored_view_states]
            changed_views = [
                v for fqn, v in current_views.items()
                if fqn in stored_view_states
                and self._has_view_changed(v[2], stored_view_states[fqn])
            ]
            deleted_view_fqns = [fqn for fqn in stored_view_states if fqn not in current_views]

            # File change detection
            new_files = [f for fid, f in current_files.items() if fid not in stored_file_states]
            changed_files = [
                f for fid, f in current_files.items()
                if fid in stored_file_states
                and self._has_file_changed(f[3], stored_file_states[fid])
            ]
            deleted_file_ids = [fid for fid in stored_file_states if fid not in current_files]

            self.logger.info(
                f"📊 Change detection: "
                f"tables(new={len(new_tables)}, changed={len(changed_tables)}, deleted={len(deleted_table_fqns)}) | "
                f"views(new={len(new_views)}, changed={len(changed_views)}, deleted={len(deleted_view_fqns)}) | "
                f"files(new={len(new_files)}, changed={len(changed_files)}, deleted={len(deleted_file_ids)})"
            )

            await self._sync_new_tables(new_tables)
            await self._sync_changed_tables(changed_tables)
            await self._handle_deleted_records(deleted_table_fqns, "table")
            self.sync_stats.tables_deleted = len(deleted_table_fqns)

            await self._sync_new_views(new_views)
            await self._sync_changed_views(changed_views)
            await self._handle_deleted_records(deleted_view_fqns, "view")
            self.sync_stats.views_deleted = len(deleted_view_fqns)

            await self._sync_new_files(new_files)
            await self._sync_changed_files(changed_files)
            await self._handle_deleted_records(deleted_file_ids, "file")
            self.sync_stats.files_deleted = len(deleted_file_ids)

            await self._save_sync_state(sync_point_key, hierarchy)

            self.logger.info("✅ [Incremental Sync] Snowflake incremental sync completed")

        except Exception as e:
            self.sync_stats.errors += 1
            self.logger.error(f"❌ [Incremental Sync] Error: {e}", exc_info=True)
            raise

    def _compute_column_signature(self, columns: list[dict[str, Any]]) -> str:
        if not columns:
            return ""
        sig = "|".join(
            f"{c.get('name', '')}:{c.get('data_type', '')}"
            for c in sorted(columns, key=lambda x: x.get('name', ''))
        )
        return hashlib.md5(sig.encode()).hexdigest()

    def _compute_definition_hash(self, definition: Optional[str]) -> str:
        if not definition:
            return ""
        return hashlib.md5(definition.encode('utf-8')).hexdigest()

    def _has_table_changed(self, table: SnowflakeTable, stored: SnowflakeTableState) -> bool:
        column_sig = self._compute_column_signature(table.columns)
        if column_sig and stored.column_signature and column_sig != stored.column_signature:
            return True
        if table.last_altered and stored.last_altered:
            if table.last_altered != stored.last_altered:
                return True
        return (
            table.row_count != stored.row_count
            or table.bytes != stored.bytes
        )

    def _has_view_changed(self, view: SnowflakeView, stored: SnowflakeViewState) -> bool:
        current_hash = self._compute_definition_hash(view.definition)
        if current_hash and stored.definition_hash:
            return current_hash != stored.definition_hash
        return False

    def _has_file_changed(self, file: SnowflakeFile, stored: SnowflakeFileState) -> bool:
        if file.last_modified and stored.last_modified:
            return file.last_modified != stored.last_modified
        return file.md5 != stored.md5

    async def _save_sync_state(self, sync_point_key: str, hierarchy) -> None:
        table_states: dict[str, dict[str, Any]] = {}
        view_states: dict[str, dict[str, Any]] = {}
        file_states: dict[str, dict[str, Any]] = {}

        for db in hierarchy.databases:
            schemas = hierarchy.schemas.get(db.name, [])
            for schema in schemas:
                schema_key = f"{db.name}.{schema.name}"
                for table in hierarchy.tables.get(schema_key, []):
                    table_states[table.fqn] = SnowflakeTableState(
                        column_signature=self._compute_column_signature(table.columns),
                        last_altered=table.last_altered,
                        row_count=table.row_count,
                        bytes=table.bytes,
                    ).model_dump()

                for view in hierarchy.views.get(schema_key, []):
                    view_states[view.fqn] = SnowflakeViewState(
                        definition_hash=self._compute_definition_hash(view.definition),
                    ).model_dump()

                for stage in hierarchy.stages.get(schema_key, []):
                    stage_key = f"{db.name}.{schema.name}.{stage.name}"
                    for file in hierarchy.files.get(stage_key, []):
                        if file.is_folder:
                            continue
                        file_id = f"{stage_key}/{file.relative_path}"
                        file_states[file_id] = SnowflakeFileState(
                            md5=file.md5,
                            size=file.size,
                            last_modified=file.last_modified,
                        ).model_dump()

        await self.sync_point.update_sync_point(
            sync_point_key,
            {
                "last_sync_time": get_epoch_timestamp_in_ms(),
                "table_states": json.dumps(table_states),
                "view_states": json.dumps(view_states),
                "file_states": json.dumps(file_states),
            }
        )
        self.logger.debug(
            f"Saved sync state: tables={len(table_states)}, "
            f"views={len(view_states)}, files={len(file_states)}"
        )

    async def _get_permissions(self) -> list[Permission]:
        return [Permission(
            type=PermissionType.OWNER,
            entity_type=EntityType.ORG,
        )]

    async def _sync_databases(self, databases: list[SnowflakeDatabase]) -> None:
        if not databases:
            return
        permissions = await self._get_permissions()
        groups = []
        for db in databases:
            rg = RecordGroup(
                name=db.name,
                external_group_id=db.name,
                group_type=RecordGroupType.SQL_DATABASE,
                connector_name=self.connector_name,
                connector_id=self.connector_id,
                description=db.comment or f"Snowflake Database: {db.name}",
            )
            groups.append((rg, permissions))
        await self.data_entities_processor.on_new_record_groups(groups)
        self.logger.info(f"Synced {len(groups)} databases")

    async def _sync_namespaces(
        self, database_name: str, schemas: list[SnowflakeSchema]
    ) -> None:
        if not schemas:
            return
        groups = []
        for schema in schemas:
            fqn = f"{database_name}.{schema.name}"
            rg = RecordGroup(
                name=schema.name,
                external_group_id=fqn,
                group_type=RecordGroupType.SQL_NAMESPACE,
                connector_name=self.connector_name,
                connector_id=self.connector_id,
                description=schema.comment or f"Snowflake Namespace: {fqn}",
                parent_external_group_id=database_name,
                inherit_permissions=True,
            )
            groups.append((rg, []))
        await self.data_entities_processor.on_new_record_groups(groups)
        self.logger.info(f"Synced {len(groups)} namespaces in {database_name}")

    async def _sync_stages(
        self, database_name: str, schema_name: str, stages: list[SnowflakeStage]
    ) -> None:
        if not stages:
            return
        groups = []
        parent_fqn = f"{database_name}.{schema_name}"
        for stage in stages:
            fqn = f"{database_name}.{schema_name}.{stage.name}"
            rg = RecordGroup(
                name=stage.name,
                external_group_id=fqn,
                group_type=RecordGroupType.STAGE,
                connector_name=self.connector_name,
                connector_id=self.connector_id,
                description=stage.comment or f"Snowflake Stage: {fqn}",
                parent_external_group_id=parent_fqn,
                inherit_permissions=True,
            )
            groups.append((rg, []))
        await self.data_entities_processor.on_new_record_groups(groups)
        self.logger.info(f"Synced {len(groups)} stages in {parent_fqn}")

    async def _process_tables_generator(
        self,
        database_name: str,
        schema_name: str,
        tables: list[SnowflakeTable],
    ) -> AsyncGenerator[tuple[Record, list[Permission]], None]:
        parent_fqn = f"{database_name}.{schema_name}"

        for table in tables:
            try:
                fqn = f"{database_name}.{schema_name}.{table.name}"
                record_id = str(uuid.uuid4())
                self._record_id_cache[fqn] = record_id

                frontend_url = os.getenv("FRONTEND_PUBLIC_URL", "").rstrip("/")
                weburl = f"{frontend_url}/record/{record_id}" if frontend_url else ""

                current_time = get_epoch_timestamp_in_ms()
                record = SQLTableRecord(
                    id=record_id,
                    record_name=table.name,
                    record_type=RecordType.SQL_TABLE,
                    record_group_type=RecordGroupType.SQL_NAMESPACE.value,
                    external_record_group_id=parent_fqn,
                    external_record_id=fqn,
                    external_revision_id=str(current_time),
                    origin=OriginTypes.CONNECTOR.value,
                    connector_name=self.connector_name,
                    connector_id=self.connector_id,
                    mime_type=MimeTypes.SQL_TABLE.value,
                    weburl=weburl,
                    source_created_at=current_time,
                    source_updated_at=current_time,
                    size_in_bytes=table.bytes or 0,
                    size_bytes=table.bytes,
                    row_count=table.row_count,
                    version=1,
                    inherit_permissions=True,
                )

                if table.foreign_keys:
                    for fk in table.foreign_keys:
                        target_schema = fk.get("references_schema", schema_name)
                        if fk.get("references_table"):
                            target_fqn = f"{database_name}.{target_schema}.{fk['references_table']}"
                            record.related_external_records.append(
                                RelatedExternalRecord(
                                    external_record_id=target_fqn,
                                    record_type=RecordType.SQL_TABLE,
                                    record_name=fk["references_table"],
                                    relation_type=RecordRelations.FOREIGN_KEY,
                                    source_column=fk.get("column"),
                                    target_column=fk.get("references_column"),
                                    child_table_name=fqn,
                                    parent_table_name=target_fqn,
                                    constraint_name=fk.get("constraint_name"),
                                )
                            )

                if self.indexing_filters and not self.indexing_filters.is_enabled(IndexingFilterKey.TABLES.value):
                    record.indexing_status = ProgressStatus.AUTO_INDEX_OFF.value

                yield (record, [])
                await asyncio.sleep(0)

            except Exception as e:
                self.logger.error(f"Error processing table {table.name}: {e}", exc_info=True)
                continue

    async def _process_views_generator(
        self,
        database_name: str,
        schema_name: str,
        views: list[SnowflakeView],
    ) -> AsyncGenerator[tuple[Record, list[Permission], SnowflakeView], None]:
        parent_fqn = f"{database_name}.{schema_name}"

        for view in views:
            try:
                fqn = f"{database_name}.{schema_name}.{view.name}"
                record_id = str(uuid.uuid4())
                self._record_id_cache[fqn] = record_id

                definition = await self._fetch_view_definition(database_name, schema_name, view.name)
                source_tables = self._parse_source_tables(definition)

                frontend_url = os.getenv("FRONTEND_PUBLIC_URL", "").rstrip("/")
                weburl = f"{frontend_url}/record/{record_id}" if frontend_url else ""

                current_time = get_epoch_timestamp_in_ms()
                record = SQLViewRecord(
                    id=record_id,
                    record_name=view.name,
                    record_type=RecordType.SQL_VIEW,
                    record_group_type=RecordGroupType.SQL_NAMESPACE.value,
                    external_record_group_id=parent_fqn,
                    external_record_id=fqn,
                    external_revision_id=str(current_time),
                    origin=OriginTypes.CONNECTOR.value,
                    connector_name=self.connector_name,
                    connector_id=self.connector_id,
                    weburl=weburl,
                    mime_type=MimeTypes.SQL_VIEW.value,
                    source_created_at=current_time,
                    source_updated_at=current_time,
                    definition=definition,
                    source_tables=source_tables,
                    version=1,
                    inherit_permissions=True,
                )

                if source_tables:
                    for source_table in source_tables:
                        if "." not in source_table:
                            table_fqn = f"{database_name}.{schema_name}.{source_table}"
                        elif source_table.count(".") == 1:
                            table_fqn = f"{database_name}.{source_table}"
                        else:
                            table_fqn = source_table
                        record.related_external_records.append(
                            RelatedExternalRecord(
                                external_record_id=table_fqn,
                                record_type=RecordType.SQL_TABLE,
                                record_name=source_table.split(".")[-1],
                                relation_type=RecordRelations.DEPENDS_ON,
                            )
                        )

                if self.indexing_filters and not self.indexing_filters.is_enabled(IndexingFilterKey.VIEWS.value):
                    record.indexing_status = ProgressStatus.AUTO_INDEX_OFF.value

                view.definition = definition
                view.source_tables = source_tables

                yield (record, [], view)
                await asyncio.sleep(0)

            except Exception as e:
                self.logger.error(f"Error processing view {view.name}: {e}", exc_info=True)
                continue

    async def _process_stage_files_generator(
        self,
        stage_fqn: str,
        files: list[SnowflakeFile],
    ) -> AsyncGenerator[tuple[FileRecord, list[Permission]], None]:
        for file in files:
            try:
                if file.is_folder:
                    continue

                file_id = f"{stage_fqn}/{file.relative_path}"
                ext = get_file_extension(file.relative_path)
                mime_type = get_mimetype_from_path(file.relative_path)

                frontend_url = os.getenv("FRONTEND_PUBLIC_URL", "").rstrip("/")
                record_id = str(uuid.uuid4())
                weburl = f"{frontend_url}/record/{record_id}" if frontend_url else ""

                current_time = get_epoch_timestamp_in_ms()
                record = FileRecord(
                    id=record_id,
                    record_name=file.file_name,
                    record_type=RecordType.FILE,
                    record_group_type=RecordGroupType.STAGE.value,
                    external_record_group_id=stage_fqn,
                    external_record_id=file_id,
                    external_revision_id=str(current_time),
                    origin=OriginTypes.CONNECTOR.value,
                    connector_name=self.connector_name,
                    connector_id=self.connector_id,
                    mime_type=mime_type,
                    is_file=True,
                    extension=ext,
                    path=file.relative_path,
                    weburl=weburl,
                    size_in_bytes=file.size,
                    parent_external_record_id=stage_fqn,
                    parent_record_type=None,
                    etag=file.md5,
                    source_created_at=current_time,
                    source_updated_at=current_time,
                    version=1,
                    inherit_permissions=True,
                )

                if self.indexing_filters and not self.indexing_filters.is_enabled(IndexingFilterKey.STAGE_FILES.value):
                    record.indexing_status = ProgressStatus.AUTO_INDEX_OFF.value

                yield (record, [])
                await asyncio.sleep(0)

            except Exception as e:
                self.logger.error(f"Error processing file {file.relative_path}: {e}", exc_info=True)
                continue

    async def _sync_tables(
        self, database_name: str, schema_name: str, tables: list[SnowflakeTable]
    ) -> None:
        if not tables:
            return
        parent_fqn = f"{database_name}.{schema_name}"
        batch: list[tuple[Record, list[Permission]]] = []
        total_synced = 0

        async for record, perms in self._process_tables_generator(
            database_name, schema_name, tables
        ):
            batch.append((record, perms))
            total_synced += 1

            if len(batch) >= self.batch_size:
                self.logger.debug(f"Processing batch of {len(batch)} tables")
                await self.data_entities_processor.on_new_records(batch)
                batch = []

        if batch:
            await self.data_entities_processor.on_new_records(batch)

        self.logger.info(f"Synced {total_synced} tables in {parent_fqn}")

    async def _sync_views(
        self, database_name: str, schema_name: str, views: list[SnowflakeView]
    ) -> None:
        if not views:
            return
        parent_fqn = f"{database_name}.{schema_name}"
        batch: list[tuple[Record, list[Permission]]] = []
        total_synced = 0

        async for record, perms, _ in self._process_views_generator(
            database_name, schema_name, views
        ):
            batch.append((record, perms))
            total_synced += 1

            if len(batch) >= self.batch_size:
                self.logger.debug(f"Processing batch of {len(batch)} views")
                await self.data_entities_processor.on_new_records(batch)
                batch = []

        if batch:
            await self.data_entities_processor.on_new_records(batch)

        self.logger.info(f"Synced {total_synced} views in {parent_fqn}")

    async def _sync_stage_files(
        self,
        database_name: str,
        schema_name: str,
        stage_name: str,
        files: list[SnowflakeFile],
    ) -> None:
        if not files:
            return
        stage_fqn = f"{database_name}.{schema_name}.{stage_name}"
        batch: list[tuple[FileRecord, list[Permission]]] = []
        total_files = 0

        async for record, perms in self._process_stage_files_generator(stage_fqn, files):
            batch.append((record, perms))
            total_files += 1

            if len(batch) >= self.batch_size:
                await self.data_entities_processor.on_new_records(batch)
                batch = []

        if batch:
            await self.data_entities_processor.on_new_records(batch)

        self.logger.info(f"Synced {total_files} files in stage {stage_fqn}")

    async def _sync_new_tables(
        self, tables: list[tuple[str, str, SnowflakeTable]]
    ) -> None:
        if not tables:
            return
        by_schema: dict[tuple[str, str], list[SnowflakeTable]] = {}
        for db_name, schema_name, table in tables:
            by_schema.setdefault((db_name, schema_name), []).append(table)
        for (db_name, schema_name), t_list in by_schema.items():
            await self._sync_tables(db_name, schema_name, t_list)
        self.sync_stats.tables_new += len(tables)

    async def _sync_changed_tables(
        self, tables: list[tuple[str, str, SnowflakeTable]]
    ) -> None:
        if not tables:
            return
        for db_name, schema_name, table in tables:
            await self._sync_updated_table(db_name, schema_name, table)
        self.sync_stats.tables_updated += len(tables)

    async def _sync_updated_table(
        self, database_name: str, schema_name: str, table: SnowflakeTable
    ) -> None:
        try:
            fqn = f"{database_name}.{schema_name}.{table.name}"
            existing_record = await self.data_entities_processor.get_record_by_external_id(
                connector_id=self.connector_id,
                external_record_id=fqn,
            )
            if not existing_record:
                self.logger.warning(f"No existing record found for updated table {fqn}, creating new")
                await self._sync_tables(database_name, schema_name, [table])
                return

            parent_fqn = f"{database_name}.{schema_name}"
            current_time = get_epoch_timestamp_in_ms()

            updated_record = SQLTableRecord(
                id=existing_record.id,
                record_name=table.name,
                record_type=RecordType.SQL_TABLE,
                record_group_type=RecordGroupType.SQL_NAMESPACE.value,
                external_record_group_id=parent_fqn,
                external_record_id=fqn,
                external_revision_id=str(current_time),
                origin=OriginTypes.CONNECTOR.value,
                connector_name=self.connector_name,
                connector_id=self.connector_id,
                mime_type=MimeTypes.SQL_TABLE.value,
                weburl=existing_record.weburl if hasattr(existing_record, 'weburl') else "",
                source_created_at=existing_record.source_created_at if hasattr(existing_record, 'source_created_at') else current_time,
                source_updated_at=current_time,
                size_in_bytes=table.bytes or 0,
                size_bytes=table.bytes,
                row_count=table.row_count,
                version=(existing_record.version or 1) + 1,
                inherit_permissions=True,
            )

            if table.foreign_keys:
                for fk in table.foreign_keys:
                    target_schema = fk.get("references_schema", schema_name)
                    if fk.get("references_table"):
                        target_fqn = f"{database_name}.{target_schema}.{fk['references_table']}"
                        updated_record.related_external_records.append(
                            RelatedExternalRecord(
                                external_record_id=target_fqn,
                                record_type=RecordType.SQL_TABLE,
                                record_name=fk["references_table"],
                                relation_type=RecordRelations.FOREIGN_KEY,
                                source_column=fk.get("column"),
                                target_column=fk.get("references_column"),
                                child_table_name=fqn,
                                parent_table_name=target_fqn,
                                constraint_name=fk.get("constraint_name"),
                            )
                        )

            if self.indexing_filters and not self.indexing_filters.is_enabled(IndexingFilterKey.TABLES.value):
                updated_record.indexing_status = ProgressStatus.AUTO_INDEX_OFF.value

            await self.data_entities_processor.on_record_content_update(updated_record)
        except Exception as e:
            self.logger.error(f"Error syncing updated table {table.name}: {e}", exc_info=True)

    async def _sync_new_views(
        self, views: list[tuple[str, str, SnowflakeView]]
    ) -> None:
        if not views:
            return
        by_schema: dict[tuple[str, str], list[SnowflakeView]] = {}
        for db_name, schema_name, view in views:
            by_schema.setdefault((db_name, schema_name), []).append(view)
        for (db_name, schema_name), v_list in by_schema.items():
            await self._sync_views(db_name, schema_name, v_list)
        self.sync_stats.views_new += len(views)

    async def _sync_changed_views(
        self, views: list[tuple[str, str, SnowflakeView]]
    ) -> None:
        if not views:
            return
        for db_name, schema_name, view in views:
            await self._sync_updated_view(db_name, schema_name, view)
        self.sync_stats.views_updated += len(views)

    async def _sync_updated_view(
        self, database_name: str, schema_name: str, view: SnowflakeView
    ) -> None:
        try:
            fqn = f"{database_name}.{schema_name}.{view.name}"
            existing_record = await self.data_entities_processor.get_record_by_external_id(
                connector_id=self.connector_id,
                external_record_id=fqn,
            )
            if not existing_record:
                self.logger.warning(f"No existing record found for updated view {fqn}, creating new")
                await self._sync_views(database_name, schema_name, [view])
                return

            parent_fqn = f"{database_name}.{schema_name}"
            current_time = get_epoch_timestamp_in_ms()

            definition = await self._fetch_view_definition(database_name, schema_name, view.name)
            source_tables = self._parse_source_tables(definition)

            updated_record = SQLViewRecord(
                id=existing_record.id,
                record_name=view.name,
                record_type=RecordType.SQL_VIEW,
                record_group_type=RecordGroupType.SQL_NAMESPACE.value,
                external_record_group_id=parent_fqn,
                external_record_id=fqn,
                external_revision_id=str(current_time),
                origin=OriginTypes.CONNECTOR.value,
                connector_name=self.connector_name,
                connector_id=self.connector_id,
                mime_type=MimeTypes.SQL_VIEW.value,
                weburl=existing_record.weburl if hasattr(existing_record, 'weburl') else "",
                source_created_at=existing_record.source_created_at if hasattr(existing_record, 'source_created_at') else current_time,
                source_updated_at=current_time,
                definition=definition,
                source_tables=source_tables,
                version=(existing_record.version or 1) + 1,
                inherit_permissions=True,
            )

            if source_tables:
                for source_table in source_tables:
                    if "." not in source_table:
                        table_fqn = f"{database_name}.{schema_name}.{source_table}"
                    elif source_table.count(".") == 1:
                        table_fqn = f"{database_name}.{source_table}"
                    else:
                        table_fqn = source_table
                    updated_record.related_external_records.append(
                        RelatedExternalRecord(
                            external_record_id=table_fqn,
                            record_type=RecordType.SQL_TABLE,
                            record_name=source_table.split(".")[-1],
                            relation_type=RecordRelations.DEPENDS_ON,
                        )
                    )

            if self.indexing_filters and not self.indexing_filters.is_enabled(IndexingFilterKey.VIEWS.value):
                updated_record.indexing_status = ProgressStatus.AUTO_INDEX_OFF.value

            await self.data_entities_processor.on_record_content_update(updated_record)
        except Exception as e:
            self.logger.error(f"Error syncing updated view {view.name}: {e}", exc_info=True)

    async def _sync_new_files(
        self, files: list[tuple[str, str, str, SnowflakeFile]]
    ) -> None:
        if not files:
            return
        by_stage: dict[tuple[str, str, str], list[SnowflakeFile]] = {}
        for db_name, schema_name, stage_name, file in files:
            by_stage.setdefault((db_name, schema_name, stage_name), []).append(file)
        for (db_name, schema_name, stage_name), f_list in by_stage.items():
            await self._sync_stage_files(db_name, schema_name, stage_name, f_list)
        self.sync_stats.files_new += len(files)

    async def _sync_changed_files(
        self, files: list[tuple[str, str, str, SnowflakeFile]]
    ) -> None:
        if not files:
            return
        for db_name, schema_name, stage_name, file in files:
            await self._sync_updated_file(db_name, schema_name, stage_name, file)
        self.sync_stats.files_updated += len(files)

    async def _sync_updated_file(
        self, database_name: str, schema_name: str, stage_name: str, file: SnowflakeFile
    ) -> None:
        try:
            stage_fqn = f"{database_name}.{schema_name}.{stage_name}"
            file_id = f"{stage_fqn}/{file.relative_path}"
            existing_record = await self.data_entities_processor.get_record_by_external_id(
                connector_id=self.connector_id,
                external_record_id=file_id,
            )
            if not existing_record:
                self.logger.warning(f"No existing record found for updated file {file_id}, creating new")
                await self._sync_stage_files(database_name, schema_name, stage_name, [file])
                return

            current_time = get_epoch_timestamp_in_ms()
            ext = get_file_extension(file.relative_path)
            mime_type = get_mimetype_from_path(file.relative_path)

            updated_record = FileRecord(
                id=existing_record.id,
                record_name=file.file_name,
                record_type=RecordType.FILE,
                record_group_type=RecordGroupType.STAGE.value,
                external_record_group_id=stage_fqn,
                external_record_id=file_id,
                external_revision_id=str(current_time),
                origin=OriginTypes.CONNECTOR.value,
                connector_name=self.connector_name,
                connector_id=self.connector_id,
                mime_type=mime_type,
                is_file=True,
                extension=ext,
                path=file.relative_path,
                weburl=existing_record.weburl if hasattr(existing_record, 'weburl') else "",
                size_in_bytes=file.size,
                parent_external_record_id=stage_fqn,
                parent_record_type=None,
                etag=file.md5,
                source_created_at=existing_record.source_created_at if hasattr(existing_record, 'source_created_at') else current_time,
                source_updated_at=current_time,
                version=(existing_record.version or 1) + 1,
                inherit_permissions=True,
            )

            if self.indexing_filters and not self.indexing_filters.is_enabled(IndexingFilterKey.STAGE_FILES.value):
                updated_record.indexing_status = ProgressStatus.AUTO_INDEX_OFF.value

            await self.data_entities_processor.on_record_content_update(updated_record)
        except Exception as e:
            self.logger.error(f"Error syncing updated file {file.relative_path}: {e}", exc_info=True)

    async def _handle_deleted_records(
        self, external_ids: list[str], label: str
    ) -> None:
        if not external_ids:
            return
        self.logger.info(f"Handling {len(external_ids)} deleted {label}s")
        for ext_id in external_ids:
            try:
                record = await self.data_entities_processor.get_record_by_external_id(
                    connector_id=self.connector_id,
                    external_record_id=ext_id,
                )
                if record and record.id:
                    await self.data_entities_processor.on_record_deleted(record.id)
                    self.logger.debug(f"Deleted {label} record: {ext_id}")
            except Exception as e:
                self.logger.warning(f"Failed to delete {label} record {ext_id}: {e}")

    async def _fetch_view_definition(
        self, database_name: str, schema_name: str, view_name: str
    ) -> Optional[str]:
        """Fetch view definition using GET_DDL."""
        if not self.data_source or not self.warehouse:
            return None

        try:
            # Quote each identifier so lowercase/mixed-case views created with
            # quoted names resolve correctly inside GET_DDL's string argument.
            fqn = quote_fqn(database_name, schema_name, view_name).replace("'", "''")
            sql = f"SELECT GET_DDL('VIEW', '{fqn}') as DDL"
            response = await self.data_source.execute_sql(
                statement=sql,
                database=database_name,
                warehouse=self.warehouse,
            )

            if response.success and response.data:
                meta = response.data.get("resultSetMetaData", {})
                row_type = meta.get("rowType", [])
                columns = [col.get("name", f"col_{i}") for i, col in enumerate(row_type)]

                rows = []
                for row in response.data.get("data", []):
                    if isinstance(row, list):
                        rows.append(dict(zip(columns, row)))
                    elif isinstance(row, dict):
                        rows.append(row)

                if rows:
                    ddl = rows[0].get("DDL")
                    if ddl:
                        return ddl

            self.logger.warning(f"GET_DDL returned no data for view {view_name}")
            return None

        except Exception as e:
            self.logger.warning(f"Failed to get view definition for {view_name}: {e}")
            return None

    def _parse_source_tables(self, definition: Optional[str]) -> list[str]:
        if not definition:
            return []

        unquoted_pattern = r'(?:FROM|JOIN)\s+([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*){0,2})'
        quoted_pattern = r'(?:FROM|JOIN)\s+"([^"]+)"(?:\."([^"]+)")?(?:\."([^"]+)")?'

        matches: list[str] = []

        for match in re.findall(unquoted_pattern, definition, re.IGNORECASE):
            if match and match.upper() not in (
                'SELECT', 'WHERE', 'GROUP', 'ORDER', 'HAVING', 'LIMIT', 'UNION', 'INTERSECT', 'EXCEPT'
            ):
                matches.append(match)

        for match in re.findall(quoted_pattern, definition, re.IGNORECASE):
            parts = [p for p in match if p]
            if parts:
                matches.append('.'.join(parts))

        return list(set(matches))

    async def _fetch_table_rows(
        self, database_name: str, schema_name: str, table_name: str,
        limit: int,
    ) -> list[list[Any]]:
        if not self.data_source or not self.warehouse:
            return []

        sql = f"SELECT * FROM {quote_fqn(database_name, schema_name, table_name)} LIMIT {limit}"
        try:
            async with self.rate_limiter:
                response = await self.data_source.execute_sql(
                    statement=sql,
                    database=database_name,
                    warehouse=self.warehouse,
                )
            if response.success and response.data:
                return response.data.get("data", [])
            self.logger.error(
                f"Row fetch failed for {database_name}.{schema_name}.{table_name}: "
                f"success={response.success} error={response.error} message={response.message}"
            )
        except Exception as e:
            self.logger.error(
                f"Row fetch raised for {database_name}.{schema_name}.{table_name}: {e}",
                exc_info=True,
            )
        return []

    async def stream_record(
        self,
        record: Record,
        user_id: Optional[str] = None,
        convertTo: Optional[str] = None,
    ) -> StreamingResponse:
        try:
            if not self.data_source:
                raise HTTPException(status_code=500, detail="Snowflake data source not initialized")

            if record.record_type == RecordType.FILE:
                stage_fqn = record.external_record_group_id
                file_path = record.external_record_id.replace(f"{stage_fqn}/", "")
                parts = stage_fqn.split(".")
                if len(parts) != 3:
                    raise HTTPException(status_code=500, detail="Invalid stage FQN")
                database, schema, stage = parts[0], parts[1], parts[2]

                response = await self.data_source.get_stage_file_stream(
                    database=database, schema=schema, stage=stage, relative_path=file_path
                )

                if response.status >= 400:
                    raise HTTPException(status_code=response.status, detail="Failed to fetch file from Snowflake stage")

                file_bytes = response.bytes()

                async def file_iterator():
                    yield file_bytes

                return create_stream_record_response(
                    file_iterator(), filename=record.record_name, mime_type=record.mime_type
                )

            elif record.record_type == RecordType.SQL_TABLE:
                parts = record.external_record_id.split(".")
                if len(parts) != 3:
                    raise HTTPException(status_code=500, detail="Invalid table FQN")
                database, schema, table = parts[0], parts[1], parts[2]

                if not self.data_fetcher:
                    raise HTTPException(status_code=500, detail="Data fetcher not ready")

                all_cols = await self.data_fetcher._fetch_all_columns_in_schema(database, schema)
                columns = all_cols.get(table, [])

                pks = await self.data_fetcher._fetch_primary_keys_in_schema(database, schema)
                primary_keys = [pk["column"] for pk in pks if pk["table"] == table]

                fks_list = await self.data_fetcher._fetch_foreign_keys_in_schema(database, schema)
                foreign_keys = []
                for fk in fks_list:
                    if fk.source_table == table:
                        foreign_keys.append({
                            "constraint_name": fk.constraint_name,
                            "column": fk.source_column,
                            "references_schema": fk.target_schema,
                            "references_table": fk.target_table,
                            "references_column": fk.target_column,
                        })

                sync_filters, _ = await load_connector_filters(
                    self.config_service, "snowflake", self.connector_id, self.logger
                )
                max_rows = min(
                    int(sync_filters.get_value(IndexingFilterKey.MAX_ROWS_PER_TABLE, default=1000)),
                    MAX_ROWS_PER_TABLE_LIMIT,
                )
                rows = await self._fetch_table_rows(database, schema, table, limit=max_rows)

                ddl = await self.data_fetcher.get_table_ddl(database, schema, table)

                data = {
                    "table_name": table,
                    "database_name": database,
                    "schema_name": schema,
                    "columns": columns,
                    "rows": rows,
                    "foreign_keys": foreign_keys,
                    "primary_keys": primary_keys,
                    "ddl": ddl,
                    "connector_name": self.connector_name.value if hasattr(self.connector_name, "value") else str(self.connector_name),
                }

                json_bytes = json.dumps(data, default=str).encode("utf-8")

                async def json_iterator():
                    yield json_bytes

                return create_stream_record_response(
                    json_iterator(), filename=f"{table}.json", mime_type=MimeTypes.SQL_TABLE.value
                )

            elif record.record_type == RecordType.SQL_VIEW:
                parts = record.external_record_id.split(".")
                if len(parts) != 3:
                    raise HTTPException(status_code=500, detail="Invalid view FQN")
                database, schema, view_name = parts[0], parts[1], parts[2]

                definition = await self._fetch_view_definition(database, schema, view_name)

                view_response = await self.data_source.get_view(
                    database=database, schema=schema, name=view_name
                )
                view_data = view_response.data if view_response.success else {}

                source_tables = self._parse_source_tables(definition)
                is_secure = view_data.get("is_secure", False) or view_data.get("isSecure", False)
                comment = view_data.get("comment") or ""

                if not definition:
                    self.logger.warning(
                        f"View {view_name} has no definition. is_secure={is_secure}."
                    )

                source_table_ddls: dict[str, str] = {}
                source_table_summaries: list[str] = []
                for src_table_fqn in source_tables:
                    src_parts = src_table_fqn.split(".")
                    if len(src_parts) == 3:
                        src_db, src_schema, src_table = src_parts
                    elif len(src_parts) == 2:
                        src_db, src_schema, src_table = database, src_parts[0], src_parts[1]
                    elif len(src_parts) == 1:
                        src_db, src_schema, src_table = database, schema, src_parts[0]
                    else:
                        continue

                    try:
                        if self.data_fetcher:
                            ddl = await self.data_fetcher.get_table_ddl(src_db, src_schema, src_table)
                            if ddl:
                                source_table_ddls[src_table_fqn] = ddl
                                summary = (
                                    f"Table {src_table_fqn}: {ddl[:500]}..."
                                    if len(ddl) > 500
                                    else f"Table {src_table_fqn}: {ddl}"
                                )
                                source_table_summaries.append(summary)
                    except Exception as e:
                        self.logger.debug(f"Could not fetch DDL for source table {src_table_fqn}: {e}")

                data = {
                    "view_name": view_name,
                    "database_name": database,
                    "schema_name": schema,
                    "definition": definition,
                    "source_tables": source_tables,
                    "source_table_ddls": source_table_ddls,
                    "source_tables_summary": "\n".join(source_table_summaries) if source_table_summaries else "",
                    "is_secure": is_secure,
                    "comment": comment,
                }

                json_bytes = json.dumps(data, default=str).encode("utf-8")

                async def json_iterator():
                    yield json_bytes

                return create_stream_record_response(
                    json_iterator(), filename=f"{view_name}.json", mime_type=MimeTypes.SQL_VIEW.value
                )

            raise HTTPException(status_code=400, detail=f"Unsupported record type: {record.record_type}")

        except HTTPException:
            raise
        except Exception as e:
            self.logger.error(f"Error streaming record: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=str(e)) from e

    def get_signed_url(self, record: Record) -> Optional[str]:
        return None

    async def test_connection_and_access(self) -> bool:
        if not self.data_source:
            return False
        try:
            response = await self.data_source.list_databases(show_limit=1)
            if response.success:
                self.logger.info("Snowflake connection test successful")
                return True
            self.logger.error(f"Connection test failed: {response.error}")
            return False
        except Exception as e:
            self.logger.error(f"Connection test failed: {e}", exc_info=True)
            return False

    def handle_webhook_notification(self, notification: dict) -> None:
        raise NotImplementedError(
            "Snowflake does not support webhook notifications. "
            "Use scheduled sync for change tracking."
        )

    async def cleanup(self) -> None:
        try:
            self.logger.info("Cleaning up Snowflake connector resources")
            self.data_source = None
            self.data_fetcher = None
            self._record_id_cache.clear()
            self.warehouse = None
            self.account_identifier = None
            self.logger.info("Snowflake connector cleanup completed")
        except Exception as e:
            self.logger.error(f"Error during Snowflake connector cleanup: {e}", exc_info=True)

    async def reindex_records(self, records: list[Record]) -> None:
        try:
            if not records:
                self.logger.info("No records to reindex")
                return

            self.logger.info(f"Starting reindex for {len(records)} Snowflake records")

            if not self.data_source:
                self.logger.error("Data source not initialized. Call init() first.")
                raise Exception("Snowflake data source not initialized")

            await self.data_entities_processor.reindex_existing_records(records)
            self.logger.info(f"Published reindex events for {len(records)} records")
        except Exception as e:
            self.logger.error(f"Error during Snowflake reindex: {e}", exc_info=True)
            raise

    async def get_filter_options(
        self,
        filter_key: str,
        page: int = 1,
        limit: int = 20,
        search: Optional[str] = None,
        cursor: Optional[str] = None,
    ) -> FilterOptionsResponse:
        try:
            page = max(1, page)
            limit = max(1, min(limit, 100))

            if cursor and cursor.isdigit():
                page = int(cursor)

            if filter_key == "databases":
                return await self._get_database_options(page, limit, search)
            elif filter_key == "schemas":
                return await self._get_schema_options(page, limit, search)
            elif filter_key == "tables":
                return await self._get_table_options(page, limit, search)
            elif filter_key == "views":
                return await self._get_view_options(page, limit, search)
            elif filter_key == "stages":
                return await self._get_stage_options(page, limit, search)
            elif filter_key == "files":
                return await self._get_file_options(page, limit, search)
            else:
                return FilterOptionsResponse(
                    success=False,
                    options=[],
                    page=page,
                    limit=limit,
                    has_more=False,
                    message=f"Unknown filter key: {filter_key}",
                )
        except Exception as e:
            self.logger.error(f"Error getting filter options for {filter_key}: {e}", exc_info=True)
            return FilterOptionsResponse(
                success=False,
                options=[],
                page=page,
                limit=limit,
                has_more=False,
                message=str(e),
            )

    def _paginate(
        self, items: list[dict[str, str]], page: int, limit: int, search: Optional[str]
    ) -> tuple[list[FilterOption], bool, Optional[str]]:
        if search:
            s = search.lower()
            items = [i for i in items if s in i["label"].lower()]
        start = (page - 1) * limit
        end = start + limit
        page_items = items[start:end]
        has_more = end < len(items)
        options = [FilterOption(id=i["id"], label=i["label"]) for i in page_items]
        next_cursor = str(page + 1) if has_more else None
        return options, has_more, next_cursor

    async def _get_database_options(
        self, page: int, limit: int, search: Optional[str] = None
    ) -> FilterOptionsResponse:
        if not self.data_source:
            return FilterOptionsResponse(
                success=False, options=[], page=page, limit=limit, has_more=False,
                message="Snowflake data source not initialized",
            )
        response = await self.data_source.list_databases()
        if not response.success:
            return FilterOptionsResponse(
                success=False, options=[], page=page, limit=limit, has_more=False,
                message=response.error or "Failed to fetch databases",
            )
        items = [
            {"id": db.get("name", ""), "label": db.get("name", "")}
            for db in (response.data or [])
            if db.get("name")
        ]
        options, has_more, cursor = self._paginate(items, page, limit, search)
        return FilterOptionsResponse(
            success=True, options=options, page=page, limit=limit,
            has_more=has_more, cursor=cursor,
        )

    async def _fetch_filter_hierarchy(
        self, include_files: bool = False, include_relationships: bool = False
    ):
        db_filter = self.sync_filters.get("databases")
        selected_dbs = db_filter.value if db_filter and db_filter.value else None
        return await self.data_fetcher.fetch_all(
            database_filter=selected_dbs,
            include_files=include_files,
            include_relationships=include_relationships,
        )

    async def _get_schema_options(
        self, page: int, limit: int, search: Optional[str] = None
    ) -> FilterOptionsResponse:
        if not self.data_fetcher:
            return FilterOptionsResponse(
                success=False, options=[], page=page, limit=limit, has_more=False,
                message="Snowflake data fetcher not initialized",
            )
        hierarchy = await self._fetch_filter_hierarchy()
        items: list[dict[str, str]] = []
        for db in hierarchy.databases:
            for schema in hierarchy.schemas.get(db.name, []):
                fqn = f"{db.name}.{schema.name}"
                items.append({"id": fqn, "label": fqn})
        options, has_more, cursor = self._paginate(items, page, limit, search)
        return FilterOptionsResponse(
            success=True, options=options, page=page, limit=limit,
            has_more=has_more, cursor=cursor,
        )

    async def _get_table_options(
        self, page: int, limit: int, search: Optional[str] = None
    ) -> FilterOptionsResponse:
        if not self.data_fetcher:
            return FilterOptionsResponse(
                success=False, options=[], page=page, limit=limit, has_more=False,
                message="Snowflake data fetcher not initialized",
            )
        hierarchy = await self._fetch_filter_hierarchy()
        items: list[dict[str, str]] = []
        for db in hierarchy.databases:
            for schema in hierarchy.schemas.get(db.name, []):
                schema_key = f"{db.name}.{schema.name}"
                for table in hierarchy.tables.get(schema_key, []):
                    fqn = f"{db.name}.{schema.name}.{table.name}"
                    items.append({"id": fqn, "label": fqn})
        options, has_more, cursor = self._paginate(items, page, limit, search)
        return FilterOptionsResponse(
            success=True, options=options, page=page, limit=limit,
            has_more=has_more, cursor=cursor,
        )

    async def _get_view_options(
        self, page: int, limit: int, search: Optional[str] = None
    ) -> FilterOptionsResponse:
        if not self.data_fetcher:
            return FilterOptionsResponse(
                success=False, options=[], page=page, limit=limit, has_more=False,
                message="Snowflake data fetcher not initialized",
            )
        hierarchy = await self._fetch_filter_hierarchy()
        items: list[dict[str, str]] = []
        for db in hierarchy.databases:
            for schema in hierarchy.schemas.get(db.name, []):
                schema_key = f"{db.name}.{schema.name}"
                for view in hierarchy.views.get(schema_key, []):
                    fqn = f"{db.name}.{schema.name}.{view.name}"
                    items.append({"id": fqn, "label": fqn})
        options, has_more, cursor = self._paginate(items, page, limit, search)
        return FilterOptionsResponse(
            success=True, options=options, page=page, limit=limit,
            has_more=has_more, cursor=cursor,
        )

    async def _get_stage_options(
        self, page: int, limit: int, search: Optional[str] = None
    ) -> FilterOptionsResponse:
        if not self.data_fetcher:
            return FilterOptionsResponse(
                success=False, options=[], page=page, limit=limit, has_more=False,
                message="Snowflake data fetcher not initialized",
            )
        hierarchy = await self._fetch_filter_hierarchy()
        items: list[dict[str, str]] = []
        for db in hierarchy.databases:
            for schema in hierarchy.schemas.get(db.name, []):
                schema_key = f"{db.name}.{schema.name}"
                for stage in hierarchy.stages.get(schema_key, []):
                    fqn = f"{db.name}.{schema.name}.{stage.name}"
                    items.append({"id": fqn, "label": fqn})
        options, has_more, cursor = self._paginate(items, page, limit, search)
        return FilterOptionsResponse(
            success=True, options=options, page=page, limit=limit,
            has_more=has_more, cursor=cursor,
        )

    async def _get_file_options(
        self, page: int, limit: int, search: Optional[str] = None
    ) -> FilterOptionsResponse:
        if not self.data_fetcher:
            return FilterOptionsResponse(
                success=False, options=[], page=page, limit=limit, has_more=False,
                message="Snowflake data fetcher not initialized",
            )
        stage_filter = self.sync_filters.get("stages")
        selected_stages = stage_filter.value if stage_filter and stage_filter.value else None

        hierarchy = await self._fetch_filter_hierarchy(include_files=True)
        items: list[dict[str, str]] = []
        for db in hierarchy.databases:
            for schema in hierarchy.schemas.get(db.name, []):
                schema_key = f"{db.name}.{schema.name}"
                for stage in hierarchy.stages.get(schema_key, []):
                    stage_fqn = f"{db.name}.{schema.name}.{stage.name}"
                    if selected_stages and stage_fqn not in selected_stages:
                        continue
                    for file in hierarchy.files.get(stage_fqn, []):
                        if file.is_folder:
                            continue
                        file_id = f"{stage_fqn}/{file.relative_path}"
                        items.append({"id": file_id, "label": file_id})
        options, has_more, cursor = self._paginate(items, page, limit, search)
        return FilterOptionsResponse(
            success=True, options=options, page=page, limit=limit,
            has_more=has_more, cursor=cursor,
        )

    @classmethod
    async def create_connector(
        cls,
        logger: Logger,
        data_store_provider: DataStoreProvider,
        config_service: ConfigurationService,
        connector_id: str,
        **kwargs,
    ) -> "SnowflakeConnector":
        data_entities_processor = DataSourceEntitiesProcessor(
            logger, data_store_provider, config_service
        )
        await data_entities_processor.initialize()
        return cls(
            logger,
            data_entities_processor,
            data_store_provider,
            config_service,
            connector_id,
            kwargs.get("scope", ConnectorScope.TEAM.value),
            kwargs.get("created_by"),
        )
