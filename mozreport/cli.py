import os
from pathlib import Path
from typing import Optional, Union

import appdirs
import attr
import cattr
import click
import toml

from .databricks import DatabricksConfig


@attr.s()
class CliConfig:
    default_template: str = attr.ib()
    databricks: DatabricksConfig = attr.ib()
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


def build_cli_config(defaults: Optional[Union[dict, CliConfig]] = None) -> CliConfig:
    if defaults is None:
        defaults = {}
    elif isinstance(defaults, CliConfig):
        defaults = cattr.unstructure(defaults)

    defaults.setdefault("default_template", CliConfig.valid_templates[0])
    defaults.setdefault("databricks", {})
    defaults["databricks"].setdefault("host", "https://dbc-caf9527b-e073.cloud.databricks.com")
    defaults["databricks"].setdefault("token", None)

    args = {}

    args["default_template"] = click.prompt(
        "Default template",
        default=defaults["default_template"])

    args["databricks"] = {
        "host": click.prompt("Databricks URL", default=defaults["databricks"]["host"]),
    }

    if not defaults["databricks"]["token"]:
        click.echo(
            f"You can create a Databricks access token by navigating to "
            f'{args["databricks"]["host"]}/#setting/account, selecting "Access Tokens", '
            f'and "Generate New Token."'
        )

    args["databricks"]["token"] = click.prompt(
            "Databricks token",
            type=str,
            default=defaults["databricks"]["token"])

    return cattr.structure(args, CliConfig)


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
    config = build_cli_config(config)
    config.save()
