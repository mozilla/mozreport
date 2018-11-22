# This is a script for computing the core product metrics for an experiment.

import re
import os
import sys

import click


def name_to_stub(name):
    """
    Makes a filename-safe stub from an arbitrary title.
    Ex.: "My Life (And Hard Times)" -> "my_life_and_hard_times"
    """
    return re.sub(r"[^A-Za-z0-9]+", "_", name).strip("_").lower()


def run_etl(slug, branches, output_path):
    from mozanalysis import metrics
    from mozanalysis.experiments import ExperimentAnalysis

    blessed_metrics = [
      metrics.EngagementAvgDailyHours,
      metrics.EngagementAvgDailyActiveHours,
      metrics.EngagementHourlyUris,
      metrics.EngagementIntensity,
    ]

    experiments = spark.table("experiments")  # noqa
    my_experiment = experiments.filter(experiments.experiment_id == slug)
    analysis = ExperimentAnalysis(my_experiment).metrics(*blessed_metrics).run()

    if not os.path.exists(os.path.dirname(output_path)):
        os.makedirs(os.path.dirname(output_path))
    analysis.to_csv(output_path, index=False)


@click.command()
@click.option("--branch", "branches", multiple=True, required=True, type=str)
@click.option("--slug", required=True, type=str)
@click.option("--uuid", required=True, type=str)
@click.option("--test", is_flag=True)
def cli(slug, uuid, branches, test):
    safe_slug = name_to_stub(slug)
    output_path = os.path.join(
        "/",
        "dbfs",
        "mozreport",
        "%s-%s" % (safe_slug, uuid),
        "summary.csv"
    )
    if test:
        print("Slug:", slug)
        print("Branches:", branches)
        print("Output path:", output_path)
        sys.exit(0)
    run_etl(slug, branches, output_path)


if __name__ == "__main__":
    cli()
