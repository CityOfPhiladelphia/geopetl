#!/bin/bash

#export LC_ALL=en_US.utf-8
#export LANG=en_US.utf-8
export LC_ALL=C.UTF-8
export LANG=C.UTF-8

POSTGRES_SDE_HOST = 'postgres-sde'

# check to make sure all environment variables that we'll need were declared.
# Check if variable does not exist or is zero-length
[[ ! -v POSTGRES_HOST || -z "$POSTGRES_HOST" ]] &&  echo "ERROR! POSTGRES_HOST env var is undeclared or empty! Exiting unsuccessfully.." && exit 1
[[ ! -v POSTGRES_USER || -z "$POSTGRES_USER" ]] &&  echo "ERROR! POSTGRES_USER env var is undeclared or empty! Exiting unsuccessfully.." && exit 1
[[ ! -v POSTGRES_PASSWORD || -z "$POSTGRES_PASSWORD" ]] &&  echo "ERROR! POSTGRES_PASSWORD env var is undeclared or empty! Exiting unsuccessfully.." && exit 1
[[ ! -v POSTGRES_DB || -z "$POSTGRES_DB" ]] &&  echo "ERROR! POSTGRES_DB env var is undeclared or empty! Exiting unsuccessfully.." && exit 1

# Not installing postgres client package to keep docker container small
# So here's a python script to use psycopg2 to see if the databse is up and ready for connections
pg_ready=$(python_pg_isready.py --host $POSTGRES_HOST --user $POSTGRES_USER --password $POSTGRES_PASSWORD --dbname $POSTGRES_DB --port 5432; echo $?)
pg_sde_ready=$(python_pg_isready.py --host $POSTGRES_SDE_HOST --user $POSTGRES_USER --password $POSTGRES_PASSWORD --dbname $POSTGRES_DB --port 5433; echo $?)

max_retry=10
counter=0
while [[ $pg_ready -ne 0 && $pg_sde_ready -ne 0 ]]
do
  [[ counter -eq $max_retry ]] && echo "Failed!" && exit 1
  echo "PostGIS database is not ready yet!!"
  sleep 3
  pg_ready=$(python_pg_isready.py --host $POSTGRES_HOST --user $POSTGRES_USER --password $POSTGRES_PASSWORD --dbname $POSTGRES_DB --port 5432; echo $?)
  pg_sde_ready=$(python_pg_isready.py --host $POSTGRES_SDE_HOST --user $POSTGRES_USER --password $POSTGRES_PASSWORD --dbname $POSTGRES_DB --port 5433; echo $?)
  ((counter++))
done
echo "Databases are ready and accepting conections."

# Note: the hostname postgis is a docker-made DNS record
# When you specify the container name in docker-compose.yml
pytest geopetl/tests/test_postgis.py \
  --user=$POSTGRES_USER \
  --pw=$POSTGRES_PASSWORD \
  --db=$POSTGRES_DB \
  --host=$POSTGRES_HOST \
  --schema="geopetl/tests/fixtures_data/schemas/point.json"

# test for postgres-sde
pytest geopetl/tests/test_postgis.py \
  --user=$POSTGRES_USER \
  --pw=$POSTGRES_PASSWORD \
  --db='geodatabase_testing' \
  --host='postgres-sde' \
  --port=5433 \
  --schema="geopetl/tests/fixtures_data/schemas/point.json"