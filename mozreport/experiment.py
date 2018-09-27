from pathlib import Path
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
