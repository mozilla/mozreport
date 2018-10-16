# This is a script for computing the core product metrics for an experiment.

import json
import re
import os

# BEGIN_BLOB
blob = """
{
    "slug": "test-slug-123",
    "uuid": "test-uuid",
    "branches": [
        "control",
        "experiment"
    ]
}
""".strip()
# END_BLOB


def name_to_stub(name):
    """
    Makes a filename-safe stub from an arbitrary title.
    Ex.: "My Life (And Hard Times)" -> "my_life_and_hard_times"
    """
    return re.sub(r"[^A-Za-z0-9]+", "_", name).strip("_").lower()


def run_etl(root="/"):
    config = json.loads(blob)
    slug = name_to_stub(config["slug"])
    output_path = os.path.join(
        root,
        "dbfs",
        "mozreport",
        "%s-%s" % (slug, config["uuid"]),
        "summary.csv")
    if not os.path.exists(os.path.dirname(output_path)):
        os.makedirs(os.path.dirname(output_path))
    with open(output_path, "wt") as f:
        f.write(blob)


if __name__ == "__main__":
    run_etl()
