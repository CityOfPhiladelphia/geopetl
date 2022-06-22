#!/bin/bash

#export LC_ALL=en_US.utf-8
#export LANG=en_US.utf-8
export LC_ALL=C.UTF-8
export LANG=C.UTF-8

export ORACLE_USER
export ORACLE_PW
export ORACLE_DB
export ORACLE_HOST

# Note: the hostname postgis is a docker-made DNS record
# When you specify the container name in docker-compose.yml
echo ""
echo "#########################################"
echo "Running tests against Oracle database..."
pytest -v /geopetl/tests/test_oraclesde.py \
  --user=$ORACLE_USER \
  --pw=$ORACLE_PASSWORD \
  --service_name=$ORACLE_DB \
  --host=$ORACLE_HOST \
  --port=1521 \
  --schema=$ORACLE_USER \
  --srid=2272
