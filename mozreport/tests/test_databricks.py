from io import BytesIO
from unittest.mock import Mock, create_autospec

import pytest
import requests

from mozreport import databricks
from mozreport.cli import CliConfig


@pytest.mark.integration
class TestDatabricksIntegration:
    @pytest.fixture
    def conf(self):
        cliconfig = CliConfig.from_file()
        return cliconfig.databricks

    @pytest.fixture
    def client(self, conf):
        return databricks.Client(conf)

    def test_file_exists(self, client):
        assert client.file_exists("/tdsmith")
        assert not client.file_exists("/this_file_definitely_does_not_exist_asdfasdfa")

    def test_file_roundtrip(self, client):
        contents = BytesIO(b"Hello, world")
        path = "/mozreport/test-1234"  # fixme
        if client.file_exists(path):
            client.delete_file(path)
        client.upload_file(contents, path)
        assert client.file_exists(path)
        client.delete_file(path)
        assert not client.file_exists(path)


class TestDatabricks:
    @pytest.fixture
    def mocked_client(self):
        config = databricks.DatabricksConfig(token="token", host="host")
        session = create_autospec(requests.Session())
        for method in ("get", "post"):
            getattr(session, method).return_value = Mock(status_code=200)
        client = databricks.Client(config=config, session=session)
        return (client, session)

    def test_upload_file(self, mocked_client):
        client, session = mocked_client
        contents = BytesIO(b"Hello, world!")
        client.upload_file(contents, "/my_file.txt")
        session.post.assert_called_once()

    def test_file_exists(self, mocked_client):
        client, session = mocked_client
        client.file_exists("/foo")
        session.get.assert_called_once()

    def test_delete_file(self, mocked_client):
        client, session = mocked_client
        client.delete_file("/foo")
        session.post.assert_called_once()
