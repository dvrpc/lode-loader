from setuptools import setup, find_packages

setup(
    name="loder",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "pip-chill",
        "psycopg2-binary",
        "python-dotenv",
        "requests",
    ],
    author="Mark Morley",
    author_email="mmorley@dvrpc.org",
    description="a tool to extract lodes/lehd tables from the census",
)
