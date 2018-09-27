from pathlib import Path
import pytest

from mozreport import cli
from mozreport.databricks import DatabricksConfig


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
        print(result.output)
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
