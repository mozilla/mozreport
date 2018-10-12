from mozreport.util import name_to_stub


class TestUtil:
    def test_name_to_stub(self):
        assert name_to_stub("My Life (and Hard Times)") == "my_life_and_hard_times"
        assert name_to_stub("#123: Foo") == "123_foo"
