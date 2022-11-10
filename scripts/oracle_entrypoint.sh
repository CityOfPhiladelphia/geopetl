#!/bin/bash

#export LC_ALL=en_US.utf-8
#export LANG=en_US.utf-8
export LC_ALL=C.UTF-8
export LANG=C.UTF-8

#psql will read the password from this variable automatically:
export PGPASSWORD=$POSTGRES_PASSWORD
# Since we're doing health checks via a subshell-made variable, we'll need to export
# our variables so they're accessible.
export ORACLE_USER
export ORACLE_PW
export ORACLE_DB
export ORACLE_HOST


echo ""
echo "#########################################"
echo "Running 2272 tests against Oracle database..."
pytest -vvv -ra --disable-warnings --showlocals --tb=native /geopetl/tests/test_oraclesde.py \
  --user=$ORACLE_USER \
  --pw=$ORACLE_PASSWORD \
  --service_name=$ORACLE_DB \
  --host=$ORACLE_HOST \
  --port=1521 \
  --schema=$ORACLE_USER \
  --srid=2272
ORACLE_EXIT_CODE=$?
if [[ "$ORACLE_EXIT_CODE" -ne "0"  ]]; then
    echo "Errors encountered in 2272 oracle tests."
    exit 1
fi

echo "2272 oracle tests done."
echo "#########################################"
echo ""

echo "#########################################"
echo "Running 4326 tests against Oracle database..."
pytest -vvv -ra --disable-warnings --showlocals --tb=native /geopetl/tests/test_oraclesde.py \
  --user=$ORACLE_USER \
  --pw=$ORACLE_PASSWORD \
  --service_name=$ORACLE_DB \
  --host=$ORACLE_HOST \
  --port=1521 \
  --schema=$ORACLE_USER \
  --srid=4326
ORACLE_EXIT_CODE=$?
if [[ "$ORACLE_EXIT_CODE" -ne "0"  ]]; then
    echo "Errors encountered in 4326 oracle tests."
    exit 1
fi

echo "4326 oracle tests done."
echo "#########################################"
echo ""

