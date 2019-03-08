from functools import partial
import os

from click.testing import CliRunner
import pytest

os.environ["MOZREPORT_TESTING"] = "1"


@pytest.fixture
def runner():
    runner = CliRunner()
    runner.invoke = partial(runner.invoke, catch_exceptions=False)
    return runner
