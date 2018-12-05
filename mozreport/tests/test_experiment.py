from pathlib import Path
import pytest

from mozreport.experiment import ExperimentConfig, generate_etl_script


class TestExperimentConfig:
    @pytest.fixture()
    def config(self):
        return ExperimentConfig(
            uuid="experiment-uuid",
            slug="experiment-slug",
        )

    def test_writes_tempfile(self, tmpdir, config):
        filename = Path(tmpdir.join("config.toml"))
        assert not filename.exists()
        config.save(filename)
        assert filename.exists()

        rehydrated = ExperimentConfig.from_file(filename)
        assert rehydrated == config

    def test_default_path(self, config, tmpdir):
        with tmpdir.as_cwd():
            config.save()
            assert (tmpdir/"mozreport.toml").exists()

    def test_creates_intermediate_path(self, tmpdir, config):
        filename = Path(tmpdir.join("foo", "bar", "config.toml"))
        config.save(filename)

    def test_generated_script(self, config):
        generated = generate_etl_script(config)
        # Test that the generated code doesn't throw a syntax error
        compile(generated, "<string>", mode="exec")
