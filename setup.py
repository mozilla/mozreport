from setuptools import setup

test_deps = [
    "coverage",
    "pytest-cov",
    "pytest",
]

extras = {
    "testing": test_deps,
}

with open("README.md") as f:
    long_description = f.read()

setup(
    name="mozreport",
    use_incremental=True,
    description="CLI for generating experiment reports",
    author="Tim D. Smith",
    author_email="tdsmith@mozilla.com",
    url="https://github.com/mozilla/mozreport",
    packages=["mozreport", "mozreport.tests"],
    package_data={
        "mozreport": [
            "templates/**/*",
            "etl_template/*",
        ],
    },
    setup_requires=["incremental"],
    install_requires=[
        "appdirs",
        "attrs",
        "cattrs",
        "click",
        "incremental",
        "requests",
        "toml"
    ],
    tests_require=test_deps,
    extras_require=extras,
    entry_points={
        "console_scripts": [
            "mozreport=mozreport.cli:cli",
        ]
    },
    long_description=long_description,
    long_description_content_type="text/markdown",
    python_requires=">=3.6",
)
