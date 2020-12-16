from setuptools import setup

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name             = 'easysqlite',
    packages         = ['easysqlite'],
    version          = '0.1.2',
    description      = 'Wrapper librray for SQLite3',
    long_description = long_description,
    long_description_content_type = "text/markdown",
    author           = 'Power_Broker',
    author_email     = 'gitstuff2@gmail.com',
    url              = 'https://github.com/PowerBroker2/easysqlite',
    download_url     = 'https://github.com/PowerBroker2/easysqlite/archive/0.1.2.tar.gz',
    keywords         = ['SQL', 'sqlite3', 'sqlite'],
    classifiers      = [],
    install_requires = []
)
