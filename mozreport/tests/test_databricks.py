from base64 import b64encode
from io import BytesIO
import time
from unittest.mock import Mock, create_autospec
from uuid import uuid4
from textwrap import dedent

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
        s = b"Hello, world"
        contents = BytesIO(s)
        path = "/mozreport/test_" + str(uuid4())
        if client.file_exists(path):
            client.delete_file(path)
        client.upload_file(contents, path)
        assert client.file_exists(path)
        assert client.get_file(path) == s
        client.delete_file(path)
        assert not client.file_exists(path)

    def test_submit_python_task(self, client):
        uuid = uuid4()
        script_path = f"/mozreport/test_{uuid}.py"
        output_path = f"/mozreport/test_{uuid}.result"
        test_script = dedent("""\
            import sys
            output_path = sys.argv[1]
            spark.conf.set("spark.databricks.queryWatchdog.enabled", False)
            colnames = spark.table("main_summary").columns
            with open(output_path, "w") as f:
                f.write(repr(colnames))
        """)
        client.upload_file(test_script, script_path)
        run_id = client.submit_python_task(
            "Test run",
            "1003-151000-grebe23",  # shared_serverless
            script_path,
            parameters=["/dbfs" + output_path]
        )
        while True:
            time.sleep(5)
            status = client.run_info(run_id)
            state = status["state"]["life_cycle_state"]
            url = status["run_page_url"]
            print(f"{state}; {url}")
            if state == "TERMINATED":
                break
        assert client.file_exists(output_path)
        client.delete_file(script_path)
        client.delete_file(output_path)


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

        session.post.return_value.status_code = 500
        with pytest.raises(databricks.DatabricksException):
            client.upload_file(contents, "/my_file.txt")

    def test_file_exists(self, mocked_client):
        client, session = mocked_client
        assert client.file_exists("/foo")
        session.get.assert_called_once()

        session.get.return_value.status_code = 500
        session.get.return_value.json.return_value = {"error_code": "invalid error code"}
        with pytest.raises(databricks.DatabricksException):
            client.file_exists("/foo")

        session.get.return_value.status_code = 404
        session.get.return_value.json.return_value = {"error_code": "RESOURCE_DOES_NOT_EXIST"}
        assert not client.file_exists("/foo")

    def test_delete_file(self, mocked_client):
        client, session = mocked_client
        client.delete_file("/foo")
        session.post.assert_called_once()

        session.post.return_value.status_code = 500
        with pytest.raises(databricks.DatabricksException):
            client.delete_file("/foo")

    def test_submit_python_task(self, mocked_client):
        client, session = mocked_client
        session.post.return_value.json.return_value = {"run_id": 1234}
        run_id = client.submit_python_task("run name", "cluster_id", "remote_path")
        assert run_id == 1234
        session.post.assert_called_once()

        session.post.return_value.status_code = 500
        with pytest.raises(databricks.DatabricksException):
            client.submit_python_task("run name", "cluster_id", "remote_path")

    def test_run_info(self, mocked_client):
        client, session = mocked_client
        client.run_info(1234)
        session.get.assert_called_once()

        session.get.return_value.status_code = 500
        with pytest.raises(databricks.DatabricksException):
            client.run_info(1234)

    def test_get_file(self, mocked_client):
        client, session = mocked_client
        megabyte = 1 << 20
        chunks = [b"A" * megabyte, b"B" * megabyte]
        session.get.return_value.json.side_effect = [
            {
                "bytes_read": megabyte,
                "data": b64encode(chunks[0]).decode("ascii"),
            }, {
                "bytes_read": megabyte,
                "data": b64encode(chunks[1]).decode("ascii"),
            }, {
                "bytes_read": 0,
                "data": "",
            }]
        assert client.get_file("/some/file") == b"".join(chunks)

        session.get.return_value.status_code = 404
        with pytest.raises(databricks.DatabricksException):
            client.get_file("/doesnt_exist")
