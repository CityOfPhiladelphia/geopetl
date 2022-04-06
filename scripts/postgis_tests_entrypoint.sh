#!/bin/bash

#export LC_ALL=en_US.utf-8
#export LANG=en_US.utf-8
export LC_ALL=C.UTF-8
export LANG=C.UTF-8

#psql will read the password from this variable automatically:
export PGPASSWORD=$POSTGRES_PASSWORD
# Since we're doing health checks via a subshell-made variable, we'll need to export
# our variables so they're accessible.
export POSTGIS_DB
export POSTGIS_HOST
export POSTGIS_USER
export POSTGIS_PASSWORD

pg_postgis_ready=$(pg_isready -h $POSTGIS_HOST -U $POSTGIS_USER -d $POSTGIS_DB &>/dev/null; echo $? )

max_retry=20
counter=0
while [[ "$pg_postgis_ready" != "0" ]]
do
  [[ counter -eq $max_retry ]] && echo "Failed!" && exit 1
  echo "PostGIS database is not ready yet!!"
  sleep 15
  pg_isready -h $POSTGIS_HOST -U $POSTGIS_USER -d $POSTGIS_DB
  pg_postgis_ready=$(pg_isready -h $POSTGIS_HOST -U $POSTGIS_USER -d $POSTGIS_DB &>/dev/null; echo $? )
#  echo "pg_postgis_ready return is:" $pg_postgis_ready
#  echo "pg_sde_ready return is" $pg_sde_ready
  ((counter++))
done
echo "Postgis database is ready and accepting conections."

python -m pytest

# Note: the hostname postgis is a docker-made DNS record
# When you specify the container name in docker-compose.yml
echo ""
echo "#########################################"
echo "Running tests against PostGIS database..."
pytest geopetl/tests/test_postgis.py \
  --user=$POSTGIS_USER \
  --pw=$POSTGIS_PASSWORD \
  --db=$POSTGIS_DB \
  --host=$POSTGIS_HOST \
  --port=5432 \
  --schema="public" \
  --column_definition="geopetl/tests/fixtures_data/schemas/point.json" \
  --srid=2272
POSTGIS_EXIT_CODE=$?
echo "Postgis test done."
echo "#########################################"
echo ""

if [[ "$SDE_EXIT_CODE" -ne "0" || "$POSTGIS_EXIT_CODE" -ne "0"  ]]; then
    echo "Errors encountered in tests."
    exit 1
else
    echo "All tests passed!"
    exit 0
fi

