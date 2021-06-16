#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-bigcommerce",
    version="1.1.1",
    description="Sync data from your BigCommerce Store",
    author="Chris Goddard",
    url="https://github.com/chrisgoddard",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_bigcommerce"],
    install_requires=[
        "singer-python==5.4.1",
        "requests==2.21.0",
        "requests-futures==0.9.9"
    ],
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
