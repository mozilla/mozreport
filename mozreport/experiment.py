from pathlib import Path
import re
from typing import List, Optional

import attr
import cattr
import toml


@attr.s
class ExperimentConfig:
    uuid: str = attr.ib()
    slug: str = attr.ib()
    branches: List[str] = attr.ib()

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


def generate_etl_script(experiment_config):
    etl_script_path = Path(__file__).parent/"etl_script.py"
    etl_script = etl_script_path.read_text()
    blob = cattr.unstructure(experiment_config)
    etl_script = re.sub(
        r"^# BEGIN_BLOB.*# END_BLOB",
        f'blob = """{blob}"""',
        etl_script,
        flags=re.MULTILINE | re.DOTALL,
    )
    return etl_script
