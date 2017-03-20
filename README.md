# geopetl

A [petl](https://github.com/alimanfoo/petl) extension for spatial data

## Installation

    pip install git+https://github.com/CityOfPhiladelphia/geopetl
    cd geopetl
    pip install -r requirements.txt

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
