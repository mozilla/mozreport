from itertools import chain, repeat
import os
from pathlib import Path
from typing import Optional, Union
import uuid

import appdirs
import attr
import cattr
import click
import toml

from .databricks import DatabricksConfig
from .experiment import ExperimentConfig


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


def build_experiment_config(defaults: Optional[Union[dict, ExperimentConfig]]) -> ExperimentConfig:
    if defaults is None:
        defaults = {}
    elif isinstance(defaults, ExperimentConfig):
        defaults = cattr.unstructure(defaults)

    args = {
        "uuid": defaults.get("uuid", uuid.uuid4())
    }

    args["slug"] = click.prompt(
        "Experiment slug",
        type=str,
        default=defaults.get("slug", None)
    )

    default_n = len(defaults["branches"]) if "branches" in defaults else 2
    n_branches = click.prompt("Number of branches", default=default_n)

    branchiter = chain(defaults.get("branches", []), repeat(None))
    branches = []
    for i, branchname in zip(range(n_branches), branchiter):
        branches.append(click.prompt(f"Branch {i+1}", type=str, default=branchname))

    args["branches"] = branches

    return cattr.structure(args, ExperimentConfig)


@click.group()
def cli():
    pass


@cli.command()
def setup():
    """Configure mozreport with your personal settings.
    """
    config = None
    try:
        config = CliConfig.from_file()
    except FileNotFoundError:
        pass
    config = build_cli_config(config)
    config.save()


@cli.command()
def new():
    """Begin a new experiment analysis.
    """
    experiment_config = None
    try:
        experiment_config = ExperimentConfig.from_file()
    except FileNotFoundError:
        pass
    experiment_config = build_experiment_config(experiment_config)
    experiment_config.save()
