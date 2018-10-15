from pathlib import Path

import pytest

from mozreport.template import Template


class TestTemplate:
    def test_find_all(self, tmpdir):
        tmpdir = Path(tmpdir)
        templates = Template.find_all()
        names = [t.name for t in templates]
        assert "rmarkdown" in names
        assert "spam" not in names

        (tmpdir/"templates"/"spam").mkdir(parents=True)
        templates = Template.find_all(tmpdir)
        names = [t.name for t in templates]
        assert "rmarkdown" in names
        assert "spam" in names

    def test_emplace(self, tmpdir):
        tmpdir = Path(tmpdir)
        template = [t for t in Template.find_all() if t.name == "rmarkdown"][0]
        with pytest.raises(FileNotFoundError):
            template.emplace(tmpdir/"foo")
        (tmpdir/"foo").touch()
        with pytest.raises(NotADirectoryError):
            template.emplace(tmpdir/"foo")
        template.emplace(tmpdir)
        assert "MOZREPORT_VERSION" not in (tmpdir/"report.Rmd").read_text()
        template.emplace(tmpdir)  # shouldn't raise
        with pytest.raises(FileExistsError):
            template.emplace(tmpdir, overwrite=False)

    def test_emplace_complex(self, tmpdir):
        tmpdir = Path(tmpdir)
        deep = tmpdir/"templates"/"spam"/"eggs"
        deep.mkdir(parents=True)
        (deep/"camelot").touch()

        template = [t for t in Template.find_all(tmpdir) if t.name == "spam"][0]
        template.emplace(tmpdir)
        assert (tmpdir/"eggs"/"camelot").exists()
