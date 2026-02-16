import json
from typing import Any, Dict

import aiohttp
import jwt

from app.config.constants.arangodb import CollectionNames
from app.config.constants.http_status_code import HttpStatusCode
from app.config.constants.service import (
    DefaultEndpoints,
    Routes,
    TokenScopes,
    config_node_constants,
)
from app.connectors.services.base_arango_service import BaseArangoService
from app.modules.transformers.transformer import TransformContext, Transformer
from app.utils.time_conversion import get_epoch_timestamp_in_ms


class BlobStorage(Transformer):
    def __init__(self,logger,config_service, arango_service: BaseArangoService = None) -> None:
        self.logger = logger
        self.config_service = config_service
        self.arango_service = arango_service

    def _clean_top_level_empty_values(self, obj: Dict[str, Any]) -> Dict[str, Any]:
        """
        Remove top-level keys with None, empty strings, empty lists, and empty dicts.
        Only processes the first level of the given object.
        """
        return {
            k: v
            for k, v in obj.items()
            if v is not None and v != "" and v != [] and v != {}
        }

    def _clean_empty_values(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Clean empty values at the top level of:
        1. The main record object
        2. Each block in block_containers.blocks
        3. Each block group in block_containers.block_groups
        """
        # Clean top-level record fields
        cleaned = self._clean_top_level_empty_values(data)

        # Clean each block's top-level fields
        if "block_containers" in cleaned and isinstance(cleaned["block_containers"], dict):
            block_containers = cleaned["block_containers"]

            if "blocks" in block_containers and isinstance(block_containers["blocks"], list):
                block_containers["blocks"] = [
                    self._clean_top_level_empty_values(block) if isinstance(block, dict) else block
                    for block in block_containers["blocks"]
                ]

            if "block_groups" in block_containers and isinstance(block_containers["block_groups"], list):
                block_containers["block_groups"] = [
                    self._clean_top_level_empty_values(bg) if isinstance(bg, dict) else bg
                    for bg in block_containers["block_groups"]
                ]

        return cleaned

    async def apply(self, ctx: TransformContext) -> TransformContext:
        record = ctx.record
        org_id = record.org_id
        record_id = record.id
        virtual_record_id = record.virtual_record_id
        # Use exclude_none=True to skip None values, then clean empty values
        record_dict = record.model_dump(mode='json', exclude_none=True)
        record_dict = self._clean_empty_values(record_dict)

        # For reconciliation updates, upload next version to existing document
        if ctx.reconciliation_context and ctx.event_type:
            from app.config.constants.arangodb import EventTypes
            if ctx.event_type == EventTypes.UPDATE_RECORD.value or ctx.event_type == EventTypes.REINDEX_RECORD.value:
                existing_result = await self.get_document_id_by_virtual_record_id(virtual_record_id)
                if existing_result:
                    # Extract record_doc_id (handles both str and list returns)
                    existing_doc_id = existing_result[0] if isinstance(existing_result, list) else existing_result
                    document_id = await self.upload_next_version(org_id, record_id, existing_doc_id, record_dict, virtual_record_id)
                else:
                    document_id = await self.save_record_to_storage(org_id, record_id, virtual_record_id, record_dict)
            else:
                document_id = await self.save_record_to_storage(org_id, record_id, virtual_record_id, record_dict)
        else:
            document_id = await self.save_record_to_storage(org_id, record_id, virtual_record_id, record_dict)

        # Store the mapping if we have both IDs and arango_service is available
        if document_id and self.arango_service:
            await self.store_virtual_record_mapping(virtual_record_id, document_id)
        ctx.record = record
        return ctx

    async def _get_signed_url(self, session, url, data, headers) -> dict | None:
        """Helper method to get signed URL with retry logic"""
        try:
            async with session.post(url, json=data, headers=headers) as response:
                if response.status != HttpStatusCode.SUCCESS.value:
                    try:
                        error_response = await response.json()
                        self.logger.error("❌ Failed to get signed URL. Status: %d, Error: %s",
                                        response.status, error_response)
                    except aiohttp.ContentTypeError:
                        error_text = await response.text()
                        self.logger.error("❌ Failed to get signed URL. Status: %d, Response: %s",
                                        response.status, error_text[:200])
                    raise aiohttp.ClientError(f"Failed with status {response.status}")

                response_data = await response.json()
                self.logger.debug("✅ Successfully retrieved signed URL")
                return response_data
        except aiohttp.ClientError as e:
            self.logger.error("❌ Network error getting signed URL: %s", str(e))
            raise
        except Exception as e:
            self.logger.error("❌ Unexpected error getting signed URL: %s", str(e))
            raise aiohttp.ClientError(f"Unexpected error: {str(e)}")

    async def _upload_to_signed_url(self, session, signed_url, data) -> int | None:
        """Helper method to upload to signed URL with retry logic"""
        try:
            async with session.put(
                signed_url,
                json=data,
                headers={"Content-Type": "application/json"}
            ) as response:
                if response.status != HttpStatusCode.SUCCESS.value:
                    try:
                        error_response = await response.json()
                        self.logger.error("❌ Failed to upload to signed URL. Status: %d, Error: %s",
                                        response.status, error_response)
                    except aiohttp.ContentTypeError:
                        error_text = await response.text()
                        self.logger.error("❌ Failed to upload to signed URL. Status: %d, Response: %s",
                                        response.status, error_text[:200])
                    raise aiohttp.ClientError(f"Failed to upload with status {response.status}")

                self.logger.debug("✅ Successfully uploaded to signed URL")
                return response.status
        except aiohttp.ClientError as e:
            self.logger.error("❌ Network error uploading to signed URL: %s", str(e))
            raise
        except Exception as e:
            self.logger.error("❌ Unexpected error uploading to signed URL: %s", str(e))
            raise aiohttp.ClientError(f"Unexpected error: {str(e)}")

    async def _create_placeholder(self, session, url, data, headers) -> dict | None:
        """Helper method to create placeholder with retry logic"""
        try:
            async with session.post(url, json=data, headers=headers) as response:
                if response.status != HttpStatusCode.SUCCESS.value:
                    try:
                        error_response = await response.json()
                        self.logger.error("❌ Failed to create placeholder. Status: %d, Error: %s",
                                        response.status, error_response)
                    except aiohttp.ContentTypeError:
                        error_text = await response.text()
                        self.logger.error("❌ Failed to create placeholder. Status: %d, Response: %s",
                                        response.status, error_text[:200])
                    raise aiohttp.ClientError(f"Failed with status {response.status}")

                response_data = await response.json()
                self.logger.debug("✅ Successfully created placeholder")
                return response_data
        except aiohttp.ClientError as e:
            self.logger.error("❌ Network error creating placeholder: %s", str(e))
            raise
        except Exception as e:
            self.logger.error("❌ Unexpected error creating placeholder: %s", str(e))
            raise aiohttp.ClientError(f"Unexpected error: {str(e)}")

    async def save_record_to_storage(self, org_id: str, record_id: str, virtual_record_id: str, record: dict) -> str | None:
        """
        Save document to storage using FormData upload
        Returns:
            str | None: document_id if successful, None if failed
        """
        try:
            self.logger.info("🚀 Starting storage process for record: %s", record_id)

            # Generate JWT token
            try:
                payload = {
                    "orgId": org_id,
                    "scopes": [TokenScopes.STORAGE_TOKEN.value],
                }
                secret_keys = await self.config_service.get_config(
                    config_node_constants.SECRET_KEYS.value
                )
                scoped_jwt_secret = secret_keys.get("scopedJwtSecret")
                if not scoped_jwt_secret:
                    raise ValueError("Missing scoped JWT secret")

                jwt_token = jwt.encode(payload, scoped_jwt_secret, algorithm="HS256")
                headers = {
                    "Authorization": f"Bearer {jwt_token}"
                }
            except Exception as e:
                self.logger.error("❌ Failed to generate JWT token: %s", str(e))
                raise e

            # Get endpoint configuration
            try:
                endpoints = await self.config_service.get_config(
                    config_node_constants.ENDPOINTS.value
                )
                nodejs_endpoint = endpoints.get("cm", {}).get("endpoint", DefaultEndpoints.NODEJS_ENDPOINT.value)
                if not nodejs_endpoint:
                    raise ValueError("Missing CM endpoint configuration")

                storage = await self.config_service.get_config(
                    config_node_constants.STORAGE.value
                )
                storage_type = storage.get("storageType")
                if not storage_type:
                    raise ValueError("Missing storage type configuration")
                self.logger.info("🚀 Storage type: %s", storage_type)
            except Exception as e:
                self.logger.error("❌ Failed to get endpoint configuration: %s", str(e))
                raise e

            if storage_type == "local":
                try:
                    async with aiohttp.ClientSession() as session:
                        upload_data = {
                            "record": record,
                            "virtualRecordId": virtual_record_id
                        }
                        json_data = json.dumps(upload_data).encode('utf-8')

                        # Create form data
                        form_data = aiohttp.FormData()
                        form_data.add_field('file',
                                        json_data,
                                        filename=f'record_{record_id}.json',
                                        content_type='application/json')
                        form_data.add_field('documentName', f'record_{record_id}')
                        form_data.add_field('documentPath', 'records')
                        form_data.add_field('isVersionedFile', 'true')
                        form_data.add_field('extension', 'json')
                        form_data.add_field('recordId', record_id)

                        # Make upload request
                        upload_url = f"{nodejs_endpoint}{Routes.STORAGE_UPLOAD.value}"
                        self.logger.info("📤 Uploading record to storage: %s", record_id)

                        async with session.post(upload_url,
                                            data=form_data,
                                            headers=headers) as response:
                            if response.status != HttpStatusCode.SUCCESS.value:
                                try:
                                    error_response = await response.json()
                                    self.logger.error("❌ Failed to upload record. Status: %d, Error: %s",
                                                    response.status, error_response)
                                except aiohttp.ContentTypeError:
                                    error_text = await response.text()
                                    self.logger.error("❌ Failed to upload record. Status: %d, Response: %s",
                                                    response.status, error_text[:200])
                                raise Exception("Failed to upload record")

                            response_data = await response.json()
                            document_id = response_data.get('_id')

                            if not document_id:
                                self.logger.error("❌ No document ID in upload response")
                                raise Exception("No document ID in upload response")

                            self.logger.info("✅ Successfully uploaded record for document: %s", document_id)
                            return document_id
                except aiohttp.ClientError as e:
                    self.logger.error("❌ Network error during upload process: %s", str(e))
                    raise e
                except Exception as e:
                    self.logger.error("❌ Unexpected error during upload process: %s", str(e))
                    self.logger.exception("Detailed error trace:")
                    raise e
            else:
                placeholder_data = {
                    "documentName": f"record_{record_id}",
                    "documentPath": "records",
                    "extension": "json"
                }

                try:
                    async with aiohttp.ClientSession() as session:
                        # Step 1: Create placeholder
                        self.logger.info("📝 Creating placeholder for record: %s", record_id)
                        placeholder_url = f"{nodejs_endpoint}{Routes.STORAGE_PLACEHOLDER.value}"
                        document = await self._create_placeholder(session, placeholder_url, placeholder_data, headers)

                        document_id = document.get("_id")
                        if not document_id:
                            self.logger.error("❌ No document ID in placeholder response")
                            raise Exception("No document ID in placeholder response")

                        self.logger.info("📄 Created placeholder with ID: %s", document_id)

                        # Step 2: Get signed URL
                        self.logger.info("🔑 Getting signed URL for document: %s", document_id)
                        upload_data = {
                            "record": record,
                            "virtualRecordId": virtual_record_id
                        }

                        upload_url = f"{nodejs_endpoint}{Routes.STORAGE_DIRECT_UPLOAD.value.format(documentId=document_id)}"
                        upload_result = await self._get_signed_url(session, upload_url, upload_data, headers)

                        signed_url = upload_result.get('signedUrl')
                        if not signed_url:
                            self.logger.error("❌ No signed URL in response for document: %s", document_id)
                            raise Exception("No signed URL in response for document")

                        # Step 3: Upload to signed URL
                        self.logger.info("📤 Uploading record to storage for document: %s", document_id)
                        await self._upload_to_signed_url(session, signed_url, upload_data)

                        self.logger.info("✅ Successfully completed record storage process for document: %s", document_id)
                        return document_id

                except aiohttp.ClientError as e:
                    self.logger.error("❌ Network error during storage process: %s", str(e))
                    raise e
                except Exception as e:
                    self.logger.error("❌ Unexpected error during storage process: %s", str(e))
                    self.logger.exception("Detailed error trace:")
                    raise e

        except Exception as e:
            self.logger.error("❌ Critical error in saving record to storage: %s", str(e))
            self.logger.exception("Detailed error trace:")
            raise e

    async def get_document_id_by_virtual_record_id(self, virtual_record_id: str):
        """
        Get the document ID(s) by virtual record ID from ArangoDB.

        For non-reconciliation record types, returns a single string (record_doc_id).
        For reconciliation-enabled record types (SQL_TABLE, SQL_VIEW), returns a list:
            [record_doc_id, record_metadata_doc_id]

        Returns:
            str | list[str] | None: document ID(s) if found, else None.
        """
        if not self.arango_service:
            self.logger.error("❌ ArangoService not initialized, cannot get document ID by virtual record ID.")
            raise Exception("ArangoService not initialized, cannot get document ID by virtual record ID.")

        try:
            collection_name = CollectionNames.VIRTUAL_RECORD_TO_DOC_ID_MAPPING.value
            query = 'FOR doc IN @@collection FILTER doc._key == @virtualRecordId RETURN doc'
            bind_vars = {
                '@collection': collection_name,
                'virtualRecordId': virtual_record_id
            }
            cursor = self.arango_service.db.aql.execute(query, bind_vars=bind_vars)

            results = list(cursor)
            if results:
                doc = results[0]
                # Support both new (record_doc_id) and legacy (documentId) field names
                record_doc_id = doc.get("record_doc_id") or doc.get("documentId")
                record_metadata_doc_id = doc.get("record_metadata_doc_id")
                if record_metadata_doc_id:
                    return [record_doc_id, record_metadata_doc_id]
                return record_doc_id
            else:
                self.logger.info("No document ID found for virtual record ID: %s", virtual_record_id)
                return None
        except Exception as e:
            self.logger.error("❌ Error getting document ID by virtual record ID: %s", str(e))
            raise e

    async def get_record_from_storage(self, virtual_record_id: str, org_id: str) -> str:
            """
            Retrieve a record's content from blob storage using the virtual_record_id.
            Returns:
                str: The content of the record if found, else an empty string.
            """
            self.logger.info("🔍 Retrieving record from storage for virtual_record_id: %s", virtual_record_id)
            try:
                # Generate JWT token for authorization
                payload = {
                    "orgId": org_id,
                    "scopes": [TokenScopes.STORAGE_TOKEN.value],
                }
                secret_keys = await self.config_service.get_config(
                    config_node_constants.SECRET_KEYS.value
                )
                scoped_jwt_secret = secret_keys.get("scopedJwtSecret")
                if not scoped_jwt_secret:
                    raise ValueError("Missing scoped JWT secret")

                jwt_token = jwt.encode(payload, scoped_jwt_secret, algorithm="HS256")
                headers = {
                    "Authorization": f"Bearer {jwt_token}"
                }

                # Get endpoint configuration
                endpoints = await self.config_service.get_config(
                    config_node_constants.ENDPOINTS.value
                )
                nodejs_endpoint = endpoints.get("cm", {}).get("endpoint", DefaultEndpoints.NODEJS_ENDPOINT.value)
                if not nodejs_endpoint:
                    raise ValueError("Missing CM endpoint configuration")

                result = await self.get_document_id_by_virtual_record_id(virtual_record_id)
                if not result:
                    self.logger.info("No document ID found for virtual record ID: %s", virtual_record_id)
                    return None

                # Extract record_doc_id (handles both str and list returns)
                document_id = result[0] if isinstance(result, list) else result

                # Build the download URL
                download_url = f"{nodejs_endpoint}{Routes.STORAGE_DOWNLOAD.value.format(documentId=document_id)}"
                async with aiohttp.ClientSession() as session:
                    async with session.get(download_url, headers=headers) as resp:
                        if resp.status == HttpStatusCode.SUCCESS.value:
                            data = await resp.json()
                            if(data.get("signedUrl")):
                                signed_url = data.get("signedUrl")
                                # Reuse the same session for signed URL fetch
                                async with session.get(signed_url, headers=headers) as resp:
                                        if resp.status == HttpStatusCode.OK.value:
                                            data = await resp.json()
                            self.logger.info("✅ Successfully retrieved record for virtual_record_id from blob storage: %s", virtual_record_id)
                            return data.get("record")
                        else:
                            self.logger.error("❌ Failed to retrieve record: status %s, virtual_record_id: %s", resp.status, virtual_record_id)
                            raise Exception("Failed to retrieve record from storage")
            except Exception as e:
                self.logger.error("❌ Error retrieving record from storage: %s", str(e))
                self.logger.exception("Detailed error trace:")
                raise e

    async def store_virtual_record_mapping(self, virtual_record_id: str, document_id: str) -> bool:
        """
        Stores the mapping between virtual_record_id and document_id in ArangoDB.
        The mapping document stores a dict with:
          - record_doc_id: the blob document ID for the record
          - (optionally) record_metadata_doc_id: the blob document ID for reconciliation metadata
        Returns:
            bool: True if successful, False otherwise.
        """

        try:
            collection_name = CollectionNames.VIRTUAL_RECORD_TO_DOC_ID_MAPPING.value

            # Create a unique key for the mapping using both IDs
            mapping_key = virtual_record_id

            mapping_document = {
                "_key": mapping_key,
                "record_doc_id": document_id,
                "updatedAt": get_epoch_timestamp_in_ms()
            }

            success = await self.arango_service.batch_upsert_nodes(
                [mapping_document],
                collection_name
            )

            if success:
                self.logger.info("✅ Successfully stored virtual record mapping: virtual_record_id=%s, document_id=%s", virtual_record_id, document_id)
                return True
            else:
                self.logger.error("❌ Failed to store virtual record mapping")
                raise Exception("Failed to store virtual record mapping")

        except Exception as e:
            self.logger.error("❌ Failed to store virtual record mapping: %s", str(e))
            self.logger.exception("Detailed error trace:")
            raise e

    async def upload_next_version(self, org_id: str, record_id: str, document_id: str, record: dict, virtual_record_id: str) -> str | None:
        """
        Upload a new version of an existing document in storage.

        Args:
            org_id: Organization ID
            record_id: Record ID
            document_id: Existing document ID to add version to
            record: Record data to upload
            virtual_record_id: Virtual record ID

        Returns:
            str | None: document_id if successful, None if failed
        """
        try:
            self.logger.info("🚀 Uploading next version for document: %s, record: %s", document_id, record_id)

            # Generate JWT token
            payload = {
                "orgId": org_id,
                "scopes": [TokenScopes.STORAGE_TOKEN.value],
            }
            secret_keys = await self.config_service.get_config(
                config_node_constants.SECRET_KEYS.value
            )
            scoped_jwt_secret = secret_keys.get("scopedJwtSecret")
            if not scoped_jwt_secret:
                raise ValueError("Missing scoped JWT secret")

            jwt_token = jwt.encode(payload, scoped_jwt_secret, algorithm="HS256")
            headers = {
                "Authorization": f"Bearer {jwt_token}"
            }

            # Get endpoint configuration
            endpoints = await self.config_service.get_config(
                config_node_constants.ENDPOINTS.value
            )
            nodejs_endpoint = endpoints.get("cm", {}).get("endpoint", DefaultEndpoints.NODEJS_ENDPOINT.value)
            if not nodejs_endpoint:
                raise ValueError("Missing CM endpoint configuration")

            upload_data = {
                "record": record,
                "virtualRecordId": virtual_record_id
            }
            json_data = json.dumps(upload_data).encode('utf-8')

            async with aiohttp.ClientSession() as session:
                form_data = aiohttp.FormData()
                form_data.add_field('file',
                                json_data,
                                filename=f'record_{record_id}.json',
                                content_type='application/json')

                upload_url = f"{nodejs_endpoint}{Routes.STORAGE_UPLOAD_NEXT_VERSION.value.format(documentId=document_id)}"
                self.logger.info("📤 Uploading next version to: %s", upload_url)

                async with session.post(upload_url, data=form_data, headers=headers) as response:
                    if response.status != HttpStatusCode.SUCCESS.value:
                        try:
                            error_response = await response.json()
                            self.logger.error("❌ Failed to upload next version. Status: %d, Error: %s",
                                            response.status, error_response)
                        except aiohttp.ContentTypeError:
                            error_text = await response.text()
                            self.logger.error("❌ Failed to upload next version. Status: %d, Response: %s",
                                            response.status, error_text[:200])
                        raise Exception("Failed to upload next version")

                    self.logger.info("✅ Successfully uploaded next version for document: %s", document_id)
                    return document_id

        except Exception as e:
            self.logger.error("❌ Error uploading next version: %s", str(e))
            raise e

    async def save_reconciliation_metadata(
        self, org_id: str, record_id: str, virtual_record_id: str, metadata_dict: dict
    ) -> str | None:
        """
        On first call, creates a new document. On subsequent calls, uploads next version.

        The metadata document ID is stored in the same virtual-record-to-doc mapping
        under the field 'record_metadata_doc_id', alongside the record's own 'record_doc_id'.

        Args:
            org_id: Organization ID
            record_id: Record ID
            virtual_record_id: Virtual record ID
            metadata_dict: Reconciliation metadata dictionary

        Returns:
            str | None: metadata document_id if successful
        """
        try:
            self.logger.info("🚀 Saving reconciliation metadata for record: %s", record_id)

            # Check if metadata document already exists in the same mapping doc
            existing_metadata_doc_id = None
            if self.arango_service:
                try:
                    collection_name = CollectionNames.VIRTUAL_RECORD_TO_DOC_ID_MAPPING.value
                    query = 'FOR doc IN @@collection FILTER doc._key == @key RETURN doc.record_metadata_doc_id'
                    bind_vars = {
                        '@collection': collection_name,
                        'key': virtual_record_id
                    }
                    cursor = self.arango_service.db.aql.execute(query, bind_vars=bind_vars)
                    results = list(cursor)
                    if results and results[0]:
                        existing_metadata_doc_id = results[0]
                except Exception as e:
                    self.logger.warning("Could not check existing metadata mapping: %s", str(e))

            if existing_metadata_doc_id:
                # Upload next version of existing metadata document
                metadata_document_id = await self.upload_next_version(
                    org_id, record_id, existing_metadata_doc_id,
                    metadata_dict, virtual_record_id
                )
            else:
                # Create new metadata document
                metadata_document_id = await self._create_metadata_document(
                    org_id, record_id, virtual_record_id, metadata_dict
                )

            # Update the same mapping document to include record_metadata_doc_id
            if metadata_document_id and self.arango_service:
                mapping_document = {
                    "_key": virtual_record_id,
                    "record_metadata_doc_id": metadata_document_id,
                    "updatedAt": get_epoch_timestamp_in_ms()
                }
                await self.arango_service.batch_upsert_nodes(
                    [mapping_document],
                    CollectionNames.VIRTUAL_RECORD_TO_DOC_ID_MAPPING.value
                )
                self.logger.info(
                    "✅ Stored metadata mapping: %s -> record_metadata_doc_id=%s",
                    virtual_record_id, metadata_document_id
                )

            return metadata_document_id

        except Exception as e:
            self.logger.error("❌ Error saving reconciliation metadata: %s", str(e))
            raise e

    async def _create_metadata_document(
        self, org_id: str, record_id: str, virtual_record_id: str, metadata_dict: dict
    ) -> str | None:
        """Create a new metadata document in blob storage.
        """
        try:
            # Generate JWT token
            payload = {
                "orgId": org_id,
                "scopes": [TokenScopes.STORAGE_TOKEN.value],
            }
            secret_keys = await self.config_service.get_config(
                config_node_constants.SECRET_KEYS.value
            )
            scoped_jwt_secret = secret_keys.get("scopedJwtSecret")
            if not scoped_jwt_secret:
                raise ValueError("Missing scoped JWT secret")

            jwt_token = jwt.encode(payload, scoped_jwt_secret, algorithm="HS256")
            headers = {
                "Authorization": f"Bearer {jwt_token}"
            }

            endpoints = await self.config_service.get_config(
                config_node_constants.ENDPOINTS.value
            )
            nodejs_endpoint = endpoints.get("cm", {}).get("endpoint", DefaultEndpoints.NODEJS_ENDPOINT.value)

            # Same shape as upload_next_version so v1 and v2+ are consistent
            upload_data = {
                "record": metadata_dict,
                "virtualRecordId": virtual_record_id,
            }
            json_data = json.dumps(upload_data).encode('utf-8')

            async with aiohttp.ClientSession() as session:
                form_data = aiohttp.FormData()
                form_data.add_field('file',
                                json_data,
                                filename=f'metadata_{record_id}.json',
                                content_type='application/json')
                form_data.add_field('documentName', f'metadata_{record_id}')
                form_data.add_field('documentPath', 'records')
                form_data.add_field('isVersionedFile', 'true')
                form_data.add_field('extension', 'json')
                form_data.add_field('recordId', record_id)

                upload_url = f"{nodejs_endpoint}{Routes.STORAGE_UPLOAD.value}"
                self.logger.info("📤 Creating metadata document for record: %s", record_id)

                async with session.post(upload_url, data=form_data, headers=headers) as response:
                    if response.status != HttpStatusCode.SUCCESS.value:
                        try:
                            error_response = await response.json()
                            self.logger.error("❌ Failed to create metadata. Status: %d, Error: %s",
                                            response.status, error_response)
                        except aiohttp.ContentTypeError:
                            error_text = await response.text()
                            self.logger.error("❌ Failed to create metadata. Status: %d, Response: %s",
                                            response.status, error_text[:200])
                        raise Exception("Failed to create metadata document")

                    response_data = await response.json()
                    document_id = response_data.get('_id')

                    if not document_id:
                        raise Exception("No document ID in metadata upload response")

                    self.logger.info("✅ Created metadata document: %s", document_id)
                    return document_id

        except Exception as e:
            self.logger.error("❌ Error creating metadata document: %s", str(e))
            raise e

    async def get_reconciliation_metadata(self, virtual_record_id: str, org_id: str) -> dict | None:
        """
        Retrieve reconciliation metadata from blob storage.

        Looks up the metadata document ID via 'record_metadata_doc_id' in the
        virtual-record-to-doc mapping for this virtual_record_id, then downloads
        the current (latest) version.

        Metadata is stored in a single format: { "record": metadata_dict, "virtualRecordId": ... }.
        Args:
            virtual_record_id: Virtual record ID
            org_id: Organization ID

        Returns:
            dict | None: Metadata dict (hash_to_block_id, block_id_to_index with block_id -> index int) if found, None otherwise
        """
        try:
            self.logger.info("🔍 Retrieving reconciliation metadata for virtual_record_id: %s", virtual_record_id)

            if not self.arango_service:
                self.logger.error("❌ ArangoService not initialized")
                return None

            try:
                collection_name = CollectionNames.VIRTUAL_RECORD_TO_DOC_ID_MAPPING.value
                query = 'FOR doc IN @@collection FILTER doc._key == @key RETURN doc.record_metadata_doc_id'
                bind_vars = {
                    '@collection': collection_name,
                    'key': virtual_record_id
                }
                cursor = self.arango_service.db.aql.execute(query, bind_vars=bind_vars)
                results = list(cursor)
                if not results or not results[0]:
                    self.logger.info("No metadata document found for virtual_record_id: %s", virtual_record_id)
                    return None
                metadata_document_id = results[0]
            except Exception as e:
                self.logger.warning("Error looking up metadata mapping: %s", str(e))
                return None

            # Download metadata from storage
            payload = {
                "orgId": org_id,
                "scopes": [TokenScopes.STORAGE_TOKEN.value],
            }
            secret_keys = await self.config_service.get_config(
                config_node_constants.SECRET_KEYS.value
            )
            scoped_jwt_secret = secret_keys.get("scopedJwtSecret")
            if not scoped_jwt_secret:
                raise ValueError("Missing scoped JWT secret")

            jwt_token = jwt.encode(payload, scoped_jwt_secret, algorithm="HS256")
            headers = {
                "Authorization": f"Bearer {jwt_token}"
            }

            endpoints = await self.config_service.get_config(
                config_node_constants.ENDPOINTS.value
            )
            nodejs_endpoint = endpoints.get("cm", {}).get("endpoint", DefaultEndpoints.NODEJS_ENDPOINT.value)

            download_url = f"{nodejs_endpoint}{Routes.STORAGE_DOWNLOAD.value.format(documentId=metadata_document_id)}"

            async with aiohttp.ClientSession() as session:
                async with session.get(download_url, headers=headers) as resp:
                    if resp.status == HttpStatusCode.SUCCESS.value:
                        data = await resp.json()
                        if data.get("signedUrl"):
                            signed_url = data.get("signedUrl")
                            async with session.get(signed_url, headers=headers) as signed_resp:
                                if signed_resp.status == HttpStatusCode.OK.value:
                                    data = await signed_resp.json()
                        # Unwrap: stored as { "record": metadata_dict, "virtualRecordId": ... }
                        if isinstance(data, dict) and "record" in data:
                            data = data.get("record", data)
                        self.logger.info(
                            "✅ Retrieved reconciliation metadata for virtual_record_id: %s",
                            virtual_record_id
                        )
                        return data
                    else:
                        self.logger.warning(
                            "⚠️ Failed to retrieve metadata: status %s, virtual_record_id: %s",
                            resp.status, virtual_record_id
                        )
                        return None

        except Exception as e:
            self.logger.error("❌ Error retrieving reconciliation metadata: %s", str(e))
            return None
