from setuptools import setup, find_packages


setup(name='geopetl',
      version='0.0.1',
      packages=find_packages(),
      install_requires=['petl'],
      extras_require={
          'oracle_sde': ['cx_Oracle'],
          'postgis': ['psycopg2'],
          'carto': ['carto'],
      }
     )
