from pathlib import Path
import sys
from typing import Optional, Union
import uuid

import attr
import cattr
import click
import toml

from .databricks import DatabricksConfig, Client
from .experiment import ExperimentConfig, generate_etl_script, submit_etl_script
from .template import Template
from .util import get_data_dir


def cli():
    pass


cli.__doc__ = f"""
    Mozreport helps you write experiment reports.

    The workflow looks like:

    * `mozreport setup` the first time you use Mozreport

    * `mozreport new` to declare a new experiment and generate an analysis script

    * `mozreport submit` to run an analysis script on Databricks

    * `mozreport fetch` to download the result

    * `mozreport report` to set up a report template

    \b
    The local configuration directory is {get_data_dir()}.
"""
cli = click.group()(cli)


@attr.s()
class CliConfig:
    default_template: str = attr.ib()
    databricks: DatabricksConfig = attr.ib()
    version: str = attr.ib(default="v1")

    valid_templates = [t.name for t in Template.find_all()]

    @default_template.validator
    def validate_default_template(self, attribute, value) -> None:
        if value not in self.valid_templates:
            raise ValueError(
                "Invalid template; choices are: %s" %
                ", ".join(self.valid_templates))

    @staticmethod
    def _default_config_path():
        return get_data_dir()/"config.toml"

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

    return cattr.structure(args, ExperimentConfig)


def get_cli_config_or_die() -> CliConfig:
    try:
        return CliConfig.from_file()
    except FileNotFoundError:
        click.echo(
            "I can't find your mozreport configuration file.\n"
            "Have you run `mozreport setup` yet?",
            err=True,
        )
        sys.exit(1)


def get_experiment_config_or_die() -> ExperimentConfig:
    try:
        return ExperimentConfig.from_file()
    except FileNotFoundError:
        click.echo(
            "I can't find an experiment configuration file in this path.\n"
            "Have you run `mozreport new` yet?",
            err=True,
        )
        sys.exit(1)


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

    click.echo("Writing ETL script to mozreport_etl_script.py...")
    script = generate_etl_script(experiment_config)
    with open("mozreport_etl_script.py", "w") as f:
        f.write(script)


@cli.command()
@click.option(
    "--cluster_slug",
    default="1003-151000-grebe23",
    help=(
        "Cluster ID (not the cluster name) of the Databricks cluster to use. "
        "Defaults to the slug for shared_serverless."
    ),
)
@click.argument("filename", default="mozreport_etl_script.py", type=click.Path(exists=True))
def submit(cluster_slug, filename):
    """Run a Python script on Databricks.

    FILENAME: The name of the file to upload and run. Defaults to mozreport_etl_script.py.
    """
    config = get_cli_config_or_die()
    experiment = get_experiment_config_or_die()
    client = Client(config.databricks)
    with open(filename, "r") as f:
        script = f.read()
    run_id = submit_etl_script(
        script,
        experiment,
        client,
        cluster_slug,
    )
    status = client.run_info(run_id)
    url = status["run_page_url"]
    click.echo("Submitted. Job status: " + url)


@cli.command()
def fetch():
    """Fetch a summary.csv file from Databricks.
    """
    config = get_cli_config_or_die()
    experiment = get_experiment_config_or_die()
    client = Client(config.databricks)
    remote_filename = experiment.dbfs_working_path + "/summary.sqlite3"
    summary = client.get_file(remote_filename)
    with open("summary.sqlite3", "wb") as f:
        f.write(summary)


@cli.command()
@click.option("--template", help="Template name to use")
def report(template):
    """Install a report template in the current working directory.
    """
    config = get_cli_config_or_die()
    all_templates = Template.find_all()
    template = template or config.default_template
    found = [t for t in all_templates if t.name == template]
    if not found:
        click.echo(f"Couldn't find template {template}.", err=True)
        click.echo("I know about: " + ','.join(t.name for t in all_templates), err=True)
        sys.exit(1)
    found[0].emplace(Path.cwd(), overwrite=False)
