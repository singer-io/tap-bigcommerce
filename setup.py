#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-bigcommerce",
    version="1.2.0",
    description="Sync data from your BigCommerce Store",
    author="Chris Goddard",
    url="https://github.com/chrisgoddard",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_bigcommerce"],
    install_requires=[
        "singer-python==6.8.0",
        "requests==2.32.5",
        "requests-futures==1.0.2"
    ],
    extras_require={
        'dev': [
            'ipdb',
            'pylint',
        ]
    },
    entry_points="""
    [console_scripts]
    tap-bigcommerce=tap_bigcommerce:main
    """,
    packages=["tap_bigcommerce"],
    package_data={
        'tap_bigcommerce': [
            'tap_bigcommerce/schemas/*.json',
            'tap_bigcommerce/schemas/shared/*.json'
        ]
    },
    include_package_data=True,
)
