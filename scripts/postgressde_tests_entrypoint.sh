#!/bin/bash


export LC_ALL=C.UTF-8
export LANG=C.UTF-8

#psql will read the password from this variable automatically:
export PGPASSWORD=$POSTGRES_PASSWORD

# Since we're doing health checks via a subshell-made variable, we'll need to export
# our variables so they're accessible.
export SDE_PASSWORD
export SDE_DB
export SDE_HOST
export SDE_USER


#pg_sde_ready=$(pg_isready -h $SDE_HOST -U $SDE_USER  -d $SDE_DB &>/dev/null; echo $? )
#pg_sde_ready=$(pg_isready -h $SDE_HOST -U $SDE_USER -d $SDE_DB &>/dev/null; echo $? )
pg_sde_ready=$(pg_isready -h $SDE_HOST -U $SDE_USER -p 5432 &>/dev/null; echo $? )
echo 'pg_sde_ready'
echo $pg_sde_ready

max_retry=20
counter=0
while [[ "$pg_sde_ready" != "0" ]]
do
  [[ counter -eq $max_retry ]] && echo "Failed!" && exit 1
  echo "postgres-sde database is not ready yet!!"
  sleep 15
  pg_sde_ready=$(pg_isready -h $SDE_HOST -U $SDE_USER -p 5432 &>/dev/null; echo $? )
  ((counter++))
done
echo "sde database ready and accepting conections."

# Note: the hostname postgis is a docker-made DNS record
# When you specify the container name in docker-compose.yml

echo "##########################################"
echo "Running 2272 postgressde tests against Esri SDE database..."
# test for postgres-sde
pytest -x geopetl/tests/test_postgressde.py \
  --user=$SDE_USER \
  --pw=$SDE_PASSWORD \
  --db=$SDE_DB \
  --host=$SDE_HOST \
  --port=5432 \
  --schema=$SDE_USER \
  --srid=2272

SDE_EXIT_CODE=$?
echo "postgres 2272 tests done."
echo "##########################################"
echo ""

if [[ "$SDE_EXIT_CODE" -ne "0" ]]; then
    echo "Errors encountered in tests."
    exit 1
fi

echo "##########################################"
echo "Running 4326 postgressde tests against Esri SDE database..."
# test for postgres-sde
pytest -x geopetl/tests/test_postgressde.py \
  --user=$SDE_USER \
  --pw=$SDE_PASSWORD \
  --db=$SDE_DB \
  --host=$SDE_HOST \
  --port=5432 \
  --schema=$SDE_USER \
  --srid=4326

