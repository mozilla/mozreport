from pathlib import Path
from typing import Optional

import attr
import cattr
import toml

from . import databricks
from .util import name_to_stub


@attr.s
class ExperimentConfig:
    uuid: str = attr.ib()
    slug: str = attr.ib()

    @staticmethod
    def _default_config_path():
        return Path("mozreport.toml")

    @classmethod
    def from_file(cls, config_path: Optional[Path] = None) -> "ExperimentConfig":
        config_path = config_path or cls._default_config_path()
        with open(config_path, "r") as f:
            blob = toml.load(f)
        return cattr.structure(blob, cls)

    def save(self, config_path: Optional[Path] = None) -> None:
        config_path = config_path or self._default_config_path()
        d = cattr.unstructure(self)
        config_path.parent.mkdir(parents=True, exist_ok=True)
        with open(config_path, "w") as f:
            toml.dump(d, f)

    @property
    def dbfs_working_path(self):
        slug = name_to_stub(self.slug)
        return f"/mozreport/{slug}-{self.uuid}"


def generate_etl_script(experiment_config):
    etl_script_path = Path(__file__).parent/"etl_template"/"etl_script.py"
    etl_script = etl_script_path.read_text()
    return etl_script


def submit_etl_script(
    etl_script: str,
    experiment: ExperimentConfig,
    client: databricks.Client,
    cluster_slug: str,
) -> None:
    remote_working_path = experiment.dbfs_working_path
    etl_script_destination = remote_working_path + "/mozreport_etl_script.py"
    if client.file_exists(etl_script_destination):
        client.delete_file(etl_script_destination)
    client.upload_file(etl_script, etl_script_destination)
    params = ["--slug", experiment.slug, "--uuid", experiment.uuid]
    job_id = client.submit_python_task(
        experiment.slug,
        cluster_slug,
        etl_script_destination,
        params
    )
    return job_id
