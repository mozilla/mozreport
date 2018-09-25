import os
from pathlib import Path
from typing import Optional, Union

import appdirs
import attr
import cattr
import click
import toml


@attr.s()
class CliConfig:
    default_template: str = attr.ib()
    version: str = attr.ib(default="v1")

    valid_templates = ["rmarkdown"]

    @default_template.validator
    def validate_default_template(self, attribute, value) -> None:
        if value not in self.valid_templates:
            raise ValueError(
                "Invalid template; choices are: %s" %
                ", ".join(self.valid_templates))

    @staticmethod
    def _default_config_path():
        parent = Path(
            os.environ.get(
                "MOZREPORT_CONFIG",
                appdirs.user_data_dir("mozreport", "Mozilla")
            ))
        return parent/"config.toml"

    @classmethod
    def from_file(cls, config_path: Optional[Path] = None) -> "CliConfig":
        """Loads a configuration file.

        Can raise FileNotFoundError.
        """
        config_path = config_path or cls._default_config_path()
        with open(config_path, "r") as f:
            blob = toml.load(f)
        return cattr.structure(blob, cls)

    def save(self, config_path: Optional[Path] = None):
        config_path = config_path or self._default_config_path()
        d = cattr.unstructure(self)
        config_path.parent.mkdir(parents=True, exist_ok=True)
        with open(config_path, "w") as f:
            toml.dump(d, f)

    @classmethod
    def from_interactive(
            cls,
            defaults: Optional[Union[dict, "CliConfig"]] = None,
            ) -> "CliConfig":
        if defaults is None:
            defaults = {}
        elif isinstance(defaults, cls):
            defaults = cattr.unstructure(defaults)

        args = {}

        args["default_template"] = click.prompt(
            "Default template",
            default=defaults.get("default_template", cls.valid_templates[0]))

        return cls(**args)


@click.group()
def cli():
    pass


@cli.command()
def setup():
    config = None
    try:
        config = CliConfig.from_file()
    except FileNotFoundError:
        pass
    config = CliConfig.from_interactive(config)
    config.save()
