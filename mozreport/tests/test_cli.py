from mozreport import cli


class TestCli:
    def test_invoke_without_args_prints_help(self, runner):
        result = runner.invoke(cli.cli)
        assert "--help" in result.output
        assert result.exit_code == 0
