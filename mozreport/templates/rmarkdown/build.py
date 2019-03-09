#!/usr/bin/env python
import subprocess


def R(script):
    return subprocess.check_call(["R", "--slave", "-e", script])


def build():
    R(
        'if(!("RSQLite" %in% .packages(all=TRUE))) '
        'install.packages("RSQLite", repo="https://cloud.r-project.org")'
    )
    R("rmarkdown::render('report.Rmd')")


if __name__ == "__main__":
    build()
