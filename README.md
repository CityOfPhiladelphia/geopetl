# geopetl

A [petl](https://github.com/alimanfoo/petl) extension for spatial data in Oracle and Postgres 



## Installation


If installing manually as a pip module, run it like this:

```
pip install git+https://github.com/CityOfPhiladelphia/geopetl
```




## Usage
**Extract (read)**  
Provides access to data from any DB-API 2.0 connection via a given query. If query = None, then defaults to  select * from table. Geopetl  
extends access to spatial data in oracle and postgres SDE enabled databases as well as postgis databases. etl.frompostgis() method is  
compatible with both postgres SDE and postgis.
petl.io.db.fromdb(dbo, query, *args, **kwargs)  
fromoraclesde(dbo, query, *args, **kwargs)  
frompostgis(dbo, query, *args, **kwargs) 

````python
import petl as etl
import geopetl
import psycopg2
import cx_Oracle

    dsn = cx_Oracle.makedsn('host', 'port', service_name='service_name')
    oracle_connection = cx_Oracle.connect('user', 'password', dsn, encoding="UTF-8") 
    oraclesde_data = etl.fromoraclesde(oracle_connection, 'oracle_table_name')

    postgisconnection = psycopg2.connect(user="postgres",
                                    password="password123",
                                    host="127.0.0.1",
                                    port="5432",
                                    database="postgis_db")
    postgis_data = etl.frompostgis(postgisconnection, 'postgis_table_name')

    postgresde_connection = psycopg2.connect(user="postgres",
                                    password="password123",
                                    host="127.0.0.1",
                                    port="5432",
                                    database="postgressde_db")
    postgressde_data = etl.frompostgis(postgresde_connection,'postgressde_table_name')
````


**Load (write)**  
Load data into an existing database table via a DB-API 2.0 connection or cursor. Note that the database table will be truncated by default.  
etl.topostgis() method is compatible with both postgres SDE and postgis.  
petl.io.db.todb(table, dbo, tablename, schema=None, commit=True, create=False, drop=False, constraints=True, metadata=None, dialect=None, sample=1000)  
tooraclesde(table, dbo, tablename,srid=None,truncate=True, increment=True)  
topostgis(table, dbo, table_name, from_srid=None,)  

```python
    import petl as etl
    import geopetl
    import psycopg2
    import cx_Oracle

    csv_data etl.fromcsv('mydata.csv')

    dsn = cx_Oracle.makedsn('host', 'port', service_name='service_name')
    oracleconnection = cx_Oracle.connect('user', 'password', dsn, encoding="UTF-8") 
    etl.tooraclesde(csv_data, oracleconnection, 'oracle_table_name') 

    postgisconnection = psycopg2.connect(user="postgres",
                                    password="password123",
                                    host="127.0.0.1",
                                    port="5432",
                                    database="postgres_db")
    etl.topostgis(csv_data, postgisconnection, 'postgis_table_name') 


    postgresde_connection = psycopg2.connect(user="postgres",
                                    password="password123",
                                    host="127.0.0.1",
                                    port="5432",
                                    database="postgres_db")
    etl.topostgis(csv_data, postgresde_connection, 'postgres_table_name') 
```
    
    
## Running tests in the docker container

To run the tests locally via docker compose, pull this version and place it in your path:

```
sudo wget https://github.com/docker/compose/releases/download/1.26.0-rc4/docker-compose-Linux-x86_64
sudo mv docker-compose-Linux-x86_64 /usr/bin/docker-compose && sudo chmod +x /usr/bin/docker-compose
```

The docker container requires oracle rpms which we have stored in S3. To get them, rename config.sh.example
to config.sh, and populate the S3 creds into the appropriate environment variables.

Then run the config like so:
`. .config.sh`
    
Pull the oracle rpms:
`. ./pull-oracle-rpm.sh`

Then run docker-compose to run start the containers and run the pytest tests 
```bash
`docker-compose -f oracle-docker-compose.yml up --build --abort-on-container-exit --exit-code-from geopetl`  
`docker-compose -f postgis-docker-compose.yml up --build --abort-on-container-exit --exit-code-from geopetl`  
`docker-compose -f postgressde-docker-compose.yml up --build --abort-on-container-exit --exit-code-from geopetl`  
`docker-compose -f sde-rds-compose.yml up --build --abort-on-container-exit --exit-code-from geopetl`
```

    

