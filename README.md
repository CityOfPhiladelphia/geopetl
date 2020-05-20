# geopetl

A [petl](https://github.com/alimanfoo/petl) extension for spatial data


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
    `docker-compose build; docker-compose up`

    

## Installation


    If installing manually as a pip module, run it like this:

    ```
    pip install git+https://github.com/CityOfPhiladelphia/geopetl
    ```

Also, make sure you have libraries for the data sources you'll be using. For example:

    pip install cx_Oracle cartodb

## Usage

    import petl as etl
    import geopetl

    (etl.read('oraclesde://user:pass@db', 'gis_streets.bike_stations')
        .rename('id', 'station_id')
        .convert('num_docks', int)
        .write('carto://domain?apikey=api_key')
    )
