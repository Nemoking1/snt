from setuptools import setup, find_packages

setup(
    name="snt-app",
    version="1.0.0",
    packages=find_packages(where="lib"),
    package_dir={"": "lib"},
    install_requires=[
        "pandas>=1.5.0",
        "openpyxl>=3.1.0",
        "plotly>=5.15.0",
    ],
)