# Automatically created by: scrapyd-deploysudo apt-get install libpq-dev


from setuptools import setup, find_packages

setup(
    name         = 'project',
    version      = '1.0',
    packages     = find_packages(),
    entry_points = {'scrapy': ['settings = scrapy_engine.settings']},
)
