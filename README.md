# mozreport [![Build Status](https://travis-ci.org/tdsmith/mozreport.svg?branch=master)](https://travis-ci.org/tdsmith/mozreport)

mozreport is a CLI tool that intends to help streamline
the process of preparing an experiment report.

## Using mozreport

```
$ mozreport --help

Usage: mozreport [OPTIONS] COMMAND [ARGS]...

  Mozreport helps you write experiment reports.

  The workflow looks like:

  * `mozreport setup` the first time you use Mozreport

  * `mozreport new` to declare a new experiment and generate an analysis
  script

  * `mozreport submit` to run an analysis script on Databricks

  * `mozreport fetch` to download the result

  * `mozreport report` to set up a report template

  The local configuration directory is /Users/tsmith/Library/Application Support/mozreport.
```

## What's a template?

A report template is any collection of code that operates on a file named `summary.csv`
in the current working directory,
and renders a report.
To add a template,
add a folder to the `mozreport/templates` folder in this repository,
or the `templates` folder inside your local configuration directory
(see the bottom of `mozreport --help`).

You may wish to adopt the convention of including a script named `build.py`
that performs the necessary steps to render the report.

## Hacking on mozreport

To run unit tests only:

`tox -- -m "not integration"`

To run all tests, including integration tests that hit our live Databricks account:

* Run `mozreport setup` once
* `tox`
