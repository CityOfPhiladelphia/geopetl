#!/bin/sh -l 


pytest geopetl/tests/test_postgis.py \
  --user=$POSTGRES_USER \
  --pw=$POSTGRES_PASSWORD \
  --db=$POSTGRES_DB \
  --schema="geopetl/tests/fixtures_data/schemas/point.json"
