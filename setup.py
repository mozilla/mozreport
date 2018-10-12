from setuptools import setup

test_deps = [
    'coverage',
    'pytest-cov',
    'pytest',
]

extras = {
    'testing': test_deps,
}

setup(
    name='mozreport',
    use_incremental=True,
    description='CLI for generating experiment reports',
    author='Tim D. Smith',
    author_email='tdsmith@mozilla.com',
    url='https://github.com/tdsmith/mozreport',
    packages=["mozreport"],
    include_package_data=True,
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
)
