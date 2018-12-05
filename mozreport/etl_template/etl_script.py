# This is a script for computing the core product metrics for an experiment.

import re
import os
import shutil
import sqlite3
import sys
import tempfile

import click


def name_to_stub(name):
    """
    Makes a filename-safe stub from an arbitrary title.
    Ex.: "My Life (And Hard Times)" -> "my_life_and_hard_times"
    """
    return re.sub(r"[^A-Za-z0-9]+", "_", name).strip("_").lower()


def run_etl(slug, enrollment_end, output_path):
    from mozanalysis import metrics
    from mozanalysis.experiments import ExperimentAnalysis
    from pyspark.sql import functions as f

    blessed_metrics = [
      metrics.EngagementAvgDailyHours,
      metrics.EngagementAvgDailyActiveHours,
      metrics.EngagementHourlyUris,
      metrics.EngagementIntensity,
    ]

    spark.conf.set("spark.databricks.queryWatchdog.enabled", False)  # noqa
    experiments = spark.table("experiments")  # noqa
    my_experiment = experiments.filter(experiments.experiment_id == slug)
    if enrollment_end:
        my_experiment = my_experiment.filter(my_experiment.submission_date_s3 > enrollment_end)
    summary = ExperimentAnalysis(my_experiment).metrics(*blessed_metrics).run()

    facets = [
        "client_id",
        "experiment_branch",
        "normalized_channel",
    ]

    columns_to_average = [
        "subsession_length",
        "active_ticks",
        "scalar_parent_browser_engagement_total_uri_count",
    ]

    per_user_daily_averages = (
        my_experiment
        .groupBy("submission_date_s3", *facets)
        .agg(
            *[f.sum(c).alias(c) for c in columns_to_average]
        )
        .groupBy(*facets)
        .agg(
            f.count("*").alias("days_active"),
            *[f.avg(c).alias(c) for c in columns_to_average]
        )
        .toPandas()
    )

    temp_db_file = tempfile.NamedTemporaryFile(delete=False)
    temp_db_path = temp_db_file.name
    temp_db_file.close()
    conn = sqlite3.connect(temp_db_path)
    summary.to_sql("summary", conn, index=False)
    per_user_daily_averages.to_sql("per_user_daily_averages", conn, index=False)
    conn.close()

    if not os.path.exists(os.path.dirname(output_path)):
        os.makedirs(os.path.dirname(output_path))
    if os.path.exists(output_path):
        os.remove(output_path)
    shutil.copy(src=temp_db_path, dst=output_path)


@click.command()
@click.option("--slug", required=True, type=str)
@click.option("--uuid", required=True, type=str)
@click.option("--enrollment-end", type=str)
@click.option("--test", is_flag=True)
def cli(slug, uuid, enrollment_end, test):
    safe_slug = name_to_stub(slug)
    output_path = os.path.join(
        "/",
        "dbfs",
        "mozreport",
        "%s-%s" % (safe_slug, uuid),
        "summary.sqlite3"
    )
    if test:
        print("Slug:", slug)
        print("Last day of enrollment period:", enrollment_end)
        print("Output path:", output_path)
        sys.exit(0)
    run_etl(slug, enrollment_end, output_path)


if __name__ == "__main__":
    cli()
    sys.exit(0)
