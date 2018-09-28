# This is a script for computing the core product metrics for an experiment.

import json
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


def run_etl(root="/"):
    config = json.loads(blob)
    output_path = os.path.join(
        root,
        "dbfs",
        "mozreport",
        "%s-%s" % (config["slug"], config["uuid"]),
        "summary.csv")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "wt") as f:
        f.write(blob)


if __name__ == "__main__":
    run_etl()
