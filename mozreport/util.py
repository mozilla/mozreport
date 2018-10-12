from pathlib import Path
import re
import os

import appdirs


def name_to_stub(name):
    """
    Makes a filename-safe stub from an arbitrary title.
    Ex.: "My Life (And Hard Times)" -> "my_life_and_hard_times"
    """
    return re.sub(r"[^A-Za-z0-9]+", "_", name).strip("_").lower()


def get_data_dir():
    return Path(
        os.environ.get(
            "MOZREPORT_CONFIG",
            appdirs.user_data_dir("mozreport", "Mozilla"),
        ))
