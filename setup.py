# coding: utf-8
# __author__: u"John"
from setuptools import setup, find_packages


setup(
    name=u"mplib",
    version=u"0.3.9",
    packages=find_packages("src"),
    package_dir={"": "src"},
    include_package_data=True,
)
