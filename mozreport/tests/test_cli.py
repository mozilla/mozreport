from pathlib import Path
from unittest.mock import Mock, create_autospec
import sys

import pytest

from mozreport import cli
from mozreport.databricks import DatabricksConfig, Client
from mozreport.experiment import ExperimentConfig


def write_config_files():
    databricks = DatabricksConfig(host="foo", token="bar")
    cliconfig = cli.CliConfig(default_template="rmarkdown", databricks=databricks)
    experiment = ExperimentConfig(uuid="monty", slug="camelot")
    cliconfig.save(Path("config.toml"))
    experiment.save()


class TestCli:
    def test_invoke_without_args_prints_help(self, runner):
        result = runner.invoke(cli.cli)
        assert "--help" in result.output
        assert result.exit_code == 0

    def test_setup(self, runner, tmpdir):
        input = "\n\n1234"
        result = runner.invoke(
            cli.cli,
            ["setup"],
            input=input,
            env={"MOZREPORT_CONFIG": str(tmpdir)},
        )
        assert result.exit_code == 0
        assert tmpdir.join("config.toml").exists()
        result = runner.invoke(
            cli.cli,
            ["setup"],
            input=input,
            env={"MOZREPORT_CONFIG": str(tmpdir)},
        )
        assert result.exit_code == 0

    def test_new(self, runner):
        input = "slug\n2\ncontrol\nexperiment\n"
        input2 = "\n\n\n\n"
        with runner.isolated_filesystem() as tmpdir:
            outfile = Path(tmpdir)/"mozreport.toml"

            result = runner.invoke(
                cli.cli,
                ["new"],
                input=input,
            )
            assert result.exit_code == 0
            assert outfile.exists()
            contents = outfile.read_bytes()

            result2 = runner.invoke(
                cli.cli,
                ["new"],
                input=input2,
            )
            assert result2.exit_code == 0
            assert contents == outfile.read_bytes()

    def test_submit(self, runner, monkeypatch):
        mock_client = create_autospec(Client)
        mock_client.return_value.submit_python_task.return_value = 1234
        monkeypatch.setattr(cli, "Client", mock_client)

        result = runner.invoke(cli.cli, ["submit", "--help"])
        assert result.exit_code == 0

        with runner.isolated_filesystem() as tmpdir:
            write_config_files()
            with open("mozreport_etl_script.py", "x") as f:
                f.write("dummy file")
            result = runner.invoke(cli.cli, ["submit"], env={"MOZREPORT_CONFIG": tmpdir})
        assert result.exit_code == 0

    def test_fetch(self, runner, monkeypatch):
        response = b"Hello, world! " + "🌎".encode("utf-8")
        mock_client = create_autospec(Client)
        mock_client.return_value.get_file.return_value = response
        monkeypatch.setattr(cli, "Client", mock_client)

        with runner.isolated_filesystem() as tmpdir:
            write_config_files()
            result = runner.invoke(cli.cli, ["fetch"], env={"MOZREPORT_CONFIG": tmpdir})
            with open("summary.sqlite3", "rb") as f:
                assert f.read() == response
        assert result.exit_code == 0

    def test_report(self, runner):
        with runner.isolated_filesystem() as tmpdir:
            write_config_files()
            result = runner.invoke(
                cli.cli,
                ["report", "--template", "rmarkdown"],
                env={"MOZREPORT_CONFIG": tmpdir},
            )
            assert result.exit_code == 0
            assert (Path(tmpdir)/"report.Rmd").exists()

        with runner.isolated_filesystem() as tmpdir:
            write_config_files()
            result = runner.invoke(
                cli.cli,
                ["report", "--template", "asdfasdf"],
                env={"MOZREPORT_CONFIG": tmpdir},
            )
            assert result.exit_code == 1
            assert "asdfasdf" in result.output


class TestConfig:
    @pytest.fixture()
    def config(self):
        return cli.CliConfig(default_template="rmarkdown", databricks=DatabricksConfig("a", "b"))

    def test_writes_tempfile(self, tmpdir, config):
        filename = Path(tmpdir.join("config.toml"))
        assert not filename.exists()
        config.save(filename)
        assert filename.exists()

        rehydrated = cli.CliConfig.from_file(filename)
        assert rehydrated == config

    def test_creates_intermediate_path(self, tmpdir, config):
        filename = Path(tmpdir.join("foo", "bar", "config.toml"))
        config.save(filename)

    def test_rejects_invalid_template(self):
        with pytest.raises(ValueError):
            cli.CliConfig(default_template="asdfasdf", databricks=DatabricksConfig("a", "b"))


class TestHelpers:
    def test_get_cli_config_or_die(self, tmpdir, monkeypatch, capsys):
        exit = Mock()
        monkeypatch.setattr(sys, "exit", exit)
        monkeypatch.setenv("MOZREPORT_CONFIG", str(tmpdir))
        with tmpdir.as_cwd():
            cli.get_cli_config_or_die()
            exit.assert_called_once()
            assert "setup" in capsys.readouterr().err

            write_config_files()
            assert isinstance(cli.get_cli_config_or_die(), cli.CliConfig)

    def test_get_experiment_config_or_die(self, tmpdir, monkeypatch, capsys):
        exit = Mock()
        monkeypatch.setattr(sys, "exit", exit)
        monkeypatch.setenv("MOZREPORT_CONFIG", str(tmpdir))
        with tmpdir.as_cwd():
            cli.get_experiment_config_or_die()
            exit.assert_called_once()
            assert "new" in capsys.readouterr().err

            write_config_files()
            assert isinstance(cli.get_experiment_config_or_die(), ExperimentConfig)
