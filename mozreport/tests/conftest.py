from functools import partial

from click.testing import CliRunner
import pytest


@pytest.fixture
def runner():
    runner = CliRunner()
    runner.invoke = partial(runner.invoke, catch_exceptions=False)
    return runner
