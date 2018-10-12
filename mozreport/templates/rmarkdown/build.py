#!/usr/bin/env python
import subprocess


def build():
    subprocess.check_call([
        "R",
        "--slave",
        "-e",
        "rmarkdown::render('report.Rmd')"
    ])


if __name__ == "__main__":
    build()
