#!/bin/sh -l 


# Note: the hostname postgis is a docker-made DNS record
# When you specify the container name in docker-compose.yml
pytest geopetl/tests/test_postgis.py \
  --user=$POSTGRES_USER \
  --pw=$POSTGRES_PASSWORD \
  --db=$POSTGRES_DB \
  --host=$POSTGRES_HOST
  --schema="geopetl/tests/fixtures_data/schemas/point.json"
