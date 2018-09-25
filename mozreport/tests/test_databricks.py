from io import BytesIO

import pytest

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
