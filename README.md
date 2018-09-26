# mozreport

To run unit tests only:

`tox -- -m "not integration"`

To run all tests, including integration tests that hit our live Databricks account:

* Run `mozreport setup` once
* `tox`
