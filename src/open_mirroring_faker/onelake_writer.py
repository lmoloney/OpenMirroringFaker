"""Upload Parquet files to the Microsoft Fabric Open Mirroring landing zone via ADLS Gen2."""

from __future__ import annotations

import json
import logging
import time
import uuid
from contextlib import suppress
from typing import Any

from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient

logger = logging.getLogger(__name__)

ONELAKE_URL = "https://onelake.dfs.fabric.microsoft.com"


def _is_retryable(exc: BaseException) -> bool:
    """Return True if the error is transient and worth retrying."""
    if isinstance(exc, (ConnectionError, TimeoutError, OSError)):
        return True
    status: int | None = getattr(exc, "status_code", None)
    return status in {429, 500, 503}


class OneLakeWriter:
    """Writes Parquet files and metadata to a Fabric Open Mirroring landing zone."""

    _MAX_RETRIES = 3

    def __init__(self, workspace_id: str, mirrored_db_id: str) -> None:
        """Connect to OneLake using DefaultAzureCredential."""
        self._workspace_id = workspace_id
        self._mirrored_db_id = mirrored_db_id

        credential = DefaultAzureCredential()
        self._service_client = DataLakeServiceClient(
            account_url=ONELAKE_URL,
            credential=credential,
        )
        self._fs_client = self._service_client.get_file_system_client(workspace_id)

        self._partner_events_written = False
        self._ensured_tables: set[tuple[str, str]] = set()

        logger.info("OneLakeWriter initialised for workspace=%s db=%s", workspace_id, mirrored_db_id)

    # -- landing-zone helpers ------------------------------------------------

    def _landing_root(self) -> str:
        return f"{self._mirrored_db_id}/Files/LandingZone"

    def _table_dir(self, schema: str, table: str) -> str:
        return f"{self._landing_root()}/{schema}.schema/{table}"

    # -- public API ----------------------------------------------------------

    def ensure_partner_events(self, partner_name: str, source_type: str) -> None:
        """Create ``_partnerEvents.json`` at the landing zone root (idempotent per session)."""
        if self._partner_events_written:
            return

        payload: dict[str, Any] = {
            "partnerName": partner_name,
            "sourceInfo": {
                "sourceType": source_type,
                "sourceVersion": "",
                "additionalInformation": {},
            },
        }

        path = f"{self._landing_root()}/_partnerEvents.json"
        file_client = self._fs_client.get_file_client(path)
        data = json.dumps(payload).encode()
        file_client.upload_data(data, overwrite=True)

        self._partner_events_written = True
        logger.info("Wrote _partnerEvents.json (partner=%s)", partner_name)

    def ensure_table(self, schema: str, table: str, key_columns: list[str]) -> None:
        """Create ``_metadata.json`` in the table folder (idempotent per session)."""
        key = (schema, table)
        if key in self._ensured_tables:
            return

        table_path = self._table_dir(schema, table)

        with suppress(Exception):
            dir_client = self._fs_client.get_directory_client(table_path)
            dir_client.create_directory()

        metadata: dict[str, Any] = {
            "keyColumns": key_columns,
            "fileDetectionStrategy": "LastUpdateTimeFileDetection",
            "isUpsertDefaultRowMarker": True,
        }

        file_client = self._fs_client.get_file_client(f"{table_path}/_metadata.json")
        data = json.dumps(metadata).encode()
        file_client.upload_data(data, overwrite=True)

        self._ensured_tables.add(key)
        logger.info("Ensured table metadata for %s.%s", schema, table)

    def upload_parquet(self, schema: str, table: str, data: bytes) -> str:
        """Upload Parquet bytes with retry. Returns the generated file name."""
        file_name = f"{uuid.uuid4()}.parquet"
        path = f"{self._table_dir(schema, table)}/{file_name}"
        file_client = self._fs_client.get_file_client(path)

        for attempt in range(self._MAX_RETRIES + 1):
            try:
                file_client.upload_data(data, overwrite=True)
                logger.debug("Uploaded %s (%d bytes)", path, len(data))
                return file_name
            except Exception as exc:
                if attempt < self._MAX_RETRIES and _is_retryable(exc):
                    delay = 1.0 * (2**attempt)
                    logger.warning(
                        "Retryable error on %s (attempt %d/%d), backing off %.1fs: %s",
                        path,
                        attempt + 1,
                        self._MAX_RETRIES,
                        delay,
                        exc,
                    )
                    time.sleep(delay)
                else:
                    raise

        raise RuntimeError("unreachable")  # pragma: no cover
