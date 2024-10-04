from setuptools import setup, find_packages

setup(
    name="delta-example",
    version="0.1",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "pyspark==3.4.0",
        "delta-spark==2.4.0",
    ],
    extras_require={
        "dev": [
            "pytest==7.3.1",
        ],
    },
)
