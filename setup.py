from setuptools import setup, find_packages

setup(
    name="data_loader",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "click",
        "pandas",
        "sqlalchemy",
        "pymysql",
        "python-dotenv"
    ],
    entry_points={
        "console_scripts": [
            "data_loader = data_loader.cli:main"  # Register `data_loader` command
        ],
    },
)