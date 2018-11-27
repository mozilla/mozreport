from base64 import b64decode
from typing.io import IO
from urllib.parse import urljoin
from typing import Optional, List

import attr
from requests import Session


@attr.s
class DatabricksConfig:
    token: str = attr.ib()
    host: str = attr.ib()


class Client:
    def __init__(self, config: DatabricksConfig, session=None) -> None:
        self.config = config

        if session is None:
            session = Session()  # pragma: no cover
        session.headers.update({"Authorization": f"Bearer {self.config.token}"})
        self._requests = session

    def upload_file(self, file: IO[bytes], remote_path: str) -> None:
        url = urljoin(self.config.host, "/api/2.0/dbfs/put")
        response = self._requests.post(
            url,
            data={
                "path": remote_path,
            },
            files={
                "contents": file,
            },
        )
        if response.status_code != 200:
            raise DatabricksException(response.text)

    def file_exists(self, remote_path: str) -> bool:
        url = urljoin(self.config.host, "/api/2.0/dbfs/get-status")
        response = self._requests.get(
            url,
            params={"path": remote_path},
        )
        if response.status_code == 200:
            return True
        body = response.json()
        if body["error_code"] == "RESOURCE_DOES_NOT_EXIST":
            return False
        raise DatabricksException(repr(body))

    def get_file(self, remote_path: str) -> bytes:
        url = urljoin(self.config.host, "/api/2.0/dbfs/read")
        chunks = []
        offset = 0
        megabyte = 1 << 20  # maximum chunk size, per api docs
        bytes_read = megabyte
        while bytes_read == megabyte:
            response = self._requests.get(
                url,
                params={
                    "path": remote_path,
                    "offset": offset,
                    "length": megabyte,
                }
            )
            if response.status_code != 200:
                raise DatabricksException(response.text)
            body = response.json()
            bytes_read = body["bytes_read"]
            offset += megabyte
            chunks.append(b64decode(body["data"]))
        return b''.join(chunks)

    def delete_file(self, remote_path: str, recursive: bool = False) -> None:
        url = urljoin(self.config.host, "/api/2.0/dbfs/delete")
        response = self._requests.post(
            url,
            json={"path": remote_path, "recursive": recursive},
        )
        if response.status_code != 200:
            raise DatabricksException(response.text)

    def submit_python_task(
        self,
        run_name: str,
        existing_cluster_id: str,
        remote_path: str,
        parameters: Optional[List[str]] = None,
    ) -> int:
        url = urljoin(self.config.host, "/api/2.0/jobs/runs/submit")
        job_definition = {
            "run_name": run_name,
            "existing_cluster_id": existing_cluster_id,
            "spark_python_task": {
                "python_file": "dbfs:" + remote_path,
                "parameters": parameters or [],
            }
        }
        response = self._requests.post(
            url,
            json=job_definition,
        )
        if response.status_code != 200:
            raise DatabricksException(response.text)
        return response.json()["run_id"]

    def run_info(self, run_id: int) -> dict:
        url = urljoin(self.config.host, "/api/2.0/jobs/runs/get")
        response = self._requests.get(
            url,
            params={"run_id": run_id},
        )
        if response.status_code != 200:
            raise DatabricksException(response.text)
        return response.json()


class DatabricksException(Exception):
    pass
