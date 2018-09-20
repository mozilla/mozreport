from typing import Dict
from typing.io import IO
from urllib.parse import urljoin

import attr
from requests import Session


@attr.s(auto_attribs=True)
class DatabricksConfig:
    token: str = attr.ib()
    host: str = "https://dbc-caf9527b-e073.cloud.databricks.com"

    @classmethod
    def from_dict(cls, d: Dict[str, str]) -> "DatabricksConfig":
        return cls(**d)

    def to_dict(self) -> Dict[str, str]:
        return attr.as_dict(self)


class Client:
    def __init__(self, config: DatabricksConfig, session=None) -> None:
        self.config = config

        if session is None:
            session = Session()
        session.headers.update({"Authorization": f"Bearer {self.config.token}"})
        self._requests = session

    def upload_file(self, file: IO[bytes], remote_path: str) -> dict:
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
        response.raise_for_status()
        return response.json()

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
        response.raise_for_status()


class DatabricksException(Exception):
    pass
