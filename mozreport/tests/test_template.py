from mozreport.template import Template


class TestTemplate:
    def test_find_all(self, tmpdir):
        templates = Template.find_all()
        names = [t.name for t in templates]
        assert "rmarkdown" in names
