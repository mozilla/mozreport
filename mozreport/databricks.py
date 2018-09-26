from typing.io import IO
from urllib.parse import urljoin

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

    def delete_file(self, remote_path: str, recursive: bool = False) -> None:
        url = urljoin(self.config.host, "/api/2.0/dbfs/delete")
        response = self._requests.post(
            url,
            json={"path": remote_path, "recursive": recursive},
        )
        if response.status_code != 200:
            raise DatabricksException(response.text)


class DatabricksException(Exception):
    pass
