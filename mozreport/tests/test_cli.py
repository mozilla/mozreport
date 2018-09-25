from pathlib import Path

from mozreport import cli


class TestCli:
    def test_invoke_without_args_prints_help(self, runner):
        result = runner.invoke(cli.cli)
        assert "--help" in result.output
        assert result.exit_code == 0

    def test_setup(self, runner, tmpdir):
        result = runner.invoke(
            cli.cli,
            ["setup"],
            input="\n",
            env={"MOZREPORT_CONFIG": str(tmpdir)},
        )
        print(result.output)
        assert result.exit_code == 0
        assert tmpdir.join("config.toml").exists()
        result = runner.invoke(
            cli.cli,
            ["setup"],
            input="\n",
            env={"MOZREPORT_CONFIG": str(tmpdir)},
        )
        assert result.exit_code == 0


class TestConfig:
    def test_writes_tempfile(self, tmpdir):
        filename = Path(tmpdir.join("config.toml"))
        assert not filename.exists()
        config = cli.CliConfig(default_template="rmarkdown")
        config.save(filename)
        assert filename.exists()

        rehydrated = cli.CliConfig.from_file(filename)
        assert rehydrated == config

    def test_creates_intermediate_path(self, tmpdir):
        filename = Path(tmpdir.join("foo", "bar", "config.toml"))
        config = cli.CliConfig(default_template="rmarkdown")
        config.save(filename)
