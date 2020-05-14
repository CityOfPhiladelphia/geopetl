from setuptools import setup, find_packages

setup(name='geopetl',
      version='0.0.0',
      packages=find_packages(),
      dependency_links = ['https://github.com/CityOfPhiladelphia/geopetl.git@#egg=geopetl-add_tests'],
      install_requires=[
            'cx-Oracle==7.3.0',
            'docker==4.2.0',
            'psycopg2-binary==2.8.4',
            'numpy==1.18.1',
            'petl==1.1.1',
            'py==1.8.1',
            'pyasn1==0.4.8',
            'pycparser==2.19',
            'pyparsing==2.4.6',
            'pytest==5.3.5',
            'python-dateutil==2.8.1'
          ],
      extras_require={
          'oracle_sde': ['cx_Oracle'],
          'postgis': ['psycopg2'],
          'carto': ['carto'],
      }
     )
