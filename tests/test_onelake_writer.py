"""Tests for open_mirroring_faker.onelake_writer."""

from __future__ import annotations

import json
from unittest.mock import patch

import pytest

from open_mirroring_faker.onelake_writer import OneLakeWriter


@pytest.fixture
def writer():
    with (
        patch("open_mirroring_faker.onelake_writer.DefaultAzureCredential"),
        patch("open_mirroring_faker.onelake_writer.DataLakeServiceClient") as mock_svc,
    ):
        mock_fs = mock_svc.return_value.get_file_system_client.return_value
        mock_file = mock_fs.get_file_client.return_value
        w = OneLakeWriter("ws-id", "db-id")
        w._mock_fs = mock_fs
        w._mock_file = mock_file
        yield w


class TestEnsurePartnerEvents:
    def test_creates_partner_events_json(self, writer: OneLakeWriter) -> None:
        writer.ensure_partner_events("TestPartner", "TestDB")

        writer._mock_file.upload_data.assert_called_once()
        uploaded = writer._mock_file.upload_data.call_args[0][0]
        payload = json.loads(uploaded)
        assert payload["partnerName"] == "TestPartner"
        assert payload["sourceInfo"]["sourceType"] == "TestDB"

    def test_idempotent(self, writer: OneLakeWriter) -> None:
        writer.ensure_partner_events("P", "S")
        writer.ensure_partner_events("P", "S")

        writer._mock_file.upload_data.assert_called_once()


class TestEnsureTableMetadata:
    def test_creates_metadata_json(self, writer: OneLakeWriter) -> None:
        writer.ensure_table("dbo", "Orders", ["OrderID"])

        writer._mock_file.upload_data.assert_called_once()
        uploaded = writer._mock_file.upload_data.call_args[0][0]
        meta = json.loads(uploaded)
        assert meta["keyColumns"] == ["OrderID"]
        assert meta["fileDetectionStrategy"] == "LastUpdateTimeFileDetection"
        assert meta["isUpsertDefaultRowMarker"] is True

    def test_idempotent(self, writer: OneLakeWriter) -> None:
        writer.ensure_table("dbo", "T", ["ID"])
        writer.ensure_table("dbo", "T", ["ID"])

        writer._mock_file.upload_data.assert_called_once()


class TestUploadParquet:
    def test_calls_upload_data(self, writer: OneLakeWriter) -> None:
        writer.upload_parquet("dbo", "T", b"parquet-bytes")

        writer._mock_file.upload_data.assert_called_once_with(b"parquet-bytes", overwrite=True)

    def test_returns_parquet_filename(self, writer: OneLakeWriter) -> None:
        name = writer.upload_parquet("dbo", "T", b"data")

        assert name.endswith(".parquet")


class TestUploadRetry:
    def test_retry_on_503(self, writer: OneLakeWriter) -> None:
        error = Exception("Service Unavailable")
        error.status_code = 503
        writer._mock_file.upload_data.side_effect = [error, None]

        with patch("open_mirroring_faker.onelake_writer.time.sleep"):
            name = writer.upload_parquet("dbo", "T", b"data")

        assert name.endswith(".parquet")
        assert writer._mock_file.upload_data.call_count == 2

    def test_no_retry_on_404(self, writer: OneLakeWriter) -> None:
        error = Exception("Not Found")
        error.status_code = 404
        writer._mock_file.upload_data.side_effect = error

        with pytest.raises(Exception, match="Not Found"):
            writer.upload_parquet("dbo", "T", b"data")

        writer._mock_file.upload_data.assert_called_once()
