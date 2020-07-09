#!/bin/bash

#export LC_ALL=en_US.utf-8
#export LANG=en_US.utf-8
export LC_ALL=C.UTF-8
export LANG=C.UTF-8

POSTGRES_SDE_HOST='postgres-sde'

# check to make sure all environment variables that we'll need were declared.
# Check if variable does not exist or is zero-length
#[[ ! -v POSTGRES_HOST || -z "$POSTGRES_HOST" ]] &&  echo "ERROR! POSTGRES_HOST env var is undeclared or empty! Exiting unsuccessfully.." && exit 1
#[[ ! -v POSTGRES_USER || -z "$POSTGRES_USER" ]] &&  echo "ERROR! POSTGRES_USER env var is undeclared or empty! Exiting unsuccessfully.." && exit 1
#[[ ! -v POSTGRES_PASSWORD || -z "$POSTGRES_PASSWORD" ]] &&  echo "ERROR! POSTGRES_PASSWORD env var is undeclared or empty! Exiting unsuccessfully.." && exit 1
#[[ ! -v POSTGRES_DB || -z "$POSTGRES_DB" ]] &&  echo "ERROR! POSTGRES_DB env var is undeclared or empty! Exiting unsuccessfully.." && exit 1

# Not installing postgres client package to keep docker container small
# So here's a python script to use psycopg2 to see if the databse is up and ready for connections
pg_postgis_ready=$(python_pg_isready.py --host $POSTGIS_HOST --user $POSTGIS_USER --password $POSTGRES_PASSWORD --dbname $POSTGIS_DB --port 5432; echo $?)
pg_sde_ready=$(python_pg_isready.py --host $SDE_HOST --user $SDE_USER --password $POSTGRES_PASSWORD --dbname $SDE_DB --port 5432; echo $?)
echo "pg_postgis_ready return is:" $pg_postgis_ready
echo "pg_sde_ready return is" $pg_sde_ready

max_retry=20
counter=0
while [[ "$pg_postgis_ready" != "0" || "$pg_sde_ready" != "0" ]]
do
  [[ counter -eq $max_retry ]] && echo "Failed!" && exit 1
  echo "PostGIS or postgres-sde database is not ready yet!!"
  sleep 15
  pg_postgis_ready=$(python_pg_isready.py --host $POSTGIS_HOST --user $POSTGIS_USER --password $POSTGRES_PASSWORD --dbname $POSTGIS_DB --port 5432; echo $?)
  pg_sde_ready=$(python_pg_isready.py --host $SDE_HOST --user $SDE_USER --password $POSTGRES_PASSWORD --dbname $SDE_DB --port 5432; echo $?)
  echo "pg_postgis_ready return is:" $pg_postgis_ready
  echo "pg_sde_ready return is" $pg_sde_ready
  ((counter++))
done
echo "Both databases are ready and accepting conections."

# Note: the hostname postgis is a docker-made DNS record
# When you specify the container name in docker-compose.yml
pytest geopetl/tests/test_postgis.py \
  --user=$POSTGIS_USER \
  --pw=$POSTGRES_PASSWORD \
  --db=$POSTGIS_DB \
  --host=$POSTGIS_HOST \
  --schema="geopetl/tests/fixtures_data/schemas/point.json"

# test for postgres-sde
pytest geopetl/tests/test_postgis.py \
  --user=$SDE_USER \
  --pw=$POSTGRES_PASSWORD \
  --db=$SDE_DB \
  --host=$SDE_HOST \
  --port=5432 \
  --schema="geopetl/tests/fixtures_data/schemas/point.json"
