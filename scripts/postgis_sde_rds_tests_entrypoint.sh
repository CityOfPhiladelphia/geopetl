#!/bin/bash

#export LC_ALL=en_US.utf-8
#export LANG=en_US.utf-8
export LC_ALL=C.UTF-8
export LANG=C.UTF-8

#psql will read the password from this variable automatically:
#export PGPASSWORD=$POSTGRES_PASSWORD
# Since we're doing health checks via a subshell-made variable, we'll need to export

echo 'postgissde entrypoint debug ....'
echo $POSTGISSDE_DB
echo $POSTGISSDE_USER
echo $POSTGISSDE_HOST
echo $POSTGISSDE_PASSWORD
echo '------------------------------'


pg_postgis_ready=$(pg_isready -h $POSTGISSDE_HOST -U $POSTGISSDE_USER -d $POSTGISSDE_DB &>/dev/null; echo $? )

max_retry=20
counter=0
while [[ "$pg_postgis_ready" != "0" ]]
do
  [[ counter -eq $max_retry ]] && echo "Failed!" && exit 1
  echo "PostGIS database is not ready yet!!"
  sleep 15
  pg_isready -h $POSTGISSDE_HOST -U $POSTGISSDE_USER -d $POSTGISSDE_DB
  pg_postgis_ready=$(pg_isready -h $POSTGISSDE_HOST -U $POSTGISSDE_USER -d $POSTGISSDE_DB &>/dev/null; echo $? )
#  echo "pg_postgis_ready return is:" $pg_postgis_ready
#  echo "pg_sde_ready return is" $pg_sde_ready
  ((counter++))
done
echo "Postgis database is ready and accepting conections."

#python -m pytest

# Note: the hostname postgis is a docker-made DNS record
# When you specify the container name in docker-compose.yml
echo ""
echo "#########################################"
echo "Running 2272 tests against PostGIS database..."
pytest -vvv -ra --disable-warnings --showlocals --tb=native geopetl/tests/test_postgis.py \
  --user=$POSTGISSDE_USER \
  --pw=$POSTGISSDE_PASSWORD \
  --db=$POSTGISSDE_DB \
  --host=$POSTGISSDE_HOST \
  --port=5432 \
  --schema=$POSTGISSDE_USER \
  --srid=2272
POSTGIS_EXIT_CODE=$?

if [[ "$POSTGIS_EXIT_CODE" -ne "0"  ]]; then
    echo "Errors encountered in 2272 postgis tests."
    exit 1
fi

echo "#########################################"
echo "Running 4326 tests against PostGIS database..."
pytest -vvv -ra --disable-warnings --showlocals --tb=native geopetl/tests/test_postgis.py \
  --user=$POSTGISSDE_USER \
  --pw=$POSTGISSDE_PASSWORD \
  --db=$POSTGISSDE_DB \
  --host=$POSTGISSDE_HOST \
  --port=5432 \
  --schema="public" \
  --srid=4326
