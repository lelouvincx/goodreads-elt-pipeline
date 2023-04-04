from setuptools import find_packages, setup

setup(
    name="elt_pipeline",
    packages=find_packages(exclude=["elt_pipeline_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagit", "pytest"]},
)
