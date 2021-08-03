import pytest
import petl as etl
from geopetl.postgis import PostgisDatabase
import psycopg2
from pytz import timezone
import csv
import os
import re

def remove_whitespace(stringval):
    shapestring = str(stringval)
    geom_type = re.findall("[A-Z]{1,12}", shapestring)[0]
    coordinates = re.findall("\d+\.\d+", shapestring)
    coordinates= [str(round(float(coords),4))for coords in coordinates]
    if geom_type == 'point' or geom_type=="POINT":
        geom = "{type}({x})".format(type=geom_type, x=" ".join(coordinates))
    elif geom_type == 'polygon' or geom_type=="POLYGON":
        geom = "{type}(({x}))".format(type=geom_type, x=" ".join(coordinates))
    return geom


def convert_to_utc(this_date):
    utc = timezone('GMT')
    eastern = timezone('US/Eastern')
    try:
        this_date = dt_parser.parse(this_date)
        this_date = this_date.astimezone(eastern)
    except:
        print('already a datetime object ....', this_date)
    return this_date
############################################# FIXTURES ################################################################

# return postgis database object
@pytest.fixture
def postgis(db, user, pw, host):
    # create connection string
    dsn = "host={0} dbname={1} user={2} password={3}".format(host,db,user,pw)
    # create & return geopetl postgis object
    postgis_db = PostgisDatabase(dsn)
    return postgis_db


# return csv file directory containing staging data
@pytest.fixture
def csv_dir():
    csv_dir = 'geopetl/tests/fixtures_data/staging/point.csv'
    return csv_dir


# return table name for postgis table based on json file name
@pytest.fixture
def table_name(csv_dir, schema):
    head_tail = os.path.split(csv_dir)
    # define which table based on csv file name
    table = ''
    if 'point' in head_tail[1]:
        table = 'point'
    elif 'polygon' in head_tail[1]:
        table = 'polygon'
    # define table name
    table_name = schema+'.'+ table + '_table'
    return table_name


# create new table and write csv staging data to it
@pytest.fixture
def create_test_tables(postgis, table_name, csv_dir, column_definition):
    # populate a new geopetl table object with staging data from csv file
    rows = etl.fromcsv(csv_dir)
    # write geopetl table to postgis
    rows.topostgis(postgis.dbo, table_name, column_definition_json=column_definition, from_srid=2272)



######################################   TESTS   ####################################################################

# compare number of rows
def test_all_rows_written(db, user, host, pw, csv_dir,create_test_tables,table_name): #
    # read staging data from csv
    with open(csv_dir, newline='') as f:
        reader = csv.reader(f)
        csv_data = list(reader)
    csv_row_count = len(csv_data[1:])

    # connect to postgis DB using psycopg2
    connection = psycopg2.connect(user=user,
                                  host=host,
                                  password=pw,
                                  database=db)
    cur = connection.cursor()
    # query all data from postgis table
    cur.execute('Select * from {table}'.format(table= table_name))
    result = cur.fetchall()

    # get number of rows from query
    postgis_num_rows = len(result)
    assert csv_row_count == postgis_num_rows


# compare csv data with postgres data using psycopg2
def test_assert_data(csv_dir, postgis, table_name):
    # read staging data from csv
    with open(csv_dir, newline='') as f:
        reader = csv.reader(f)
        csv_data1 = list(reader)

    csv_data =[]
    # remove object_id from csv_data for now
    for r in csv_data1:
        print(r[1:])
        csv_data.append(r[1:])
    # list of column names
    csv_header = csv_data[0]

    # read data using postgis
    cur = postgis.dbo.cursor()
    cur.execute('select textfield,datefield,numericfield,st_astext(shape) as shape, timezone from {table}'.format(table= table_name))
    rows = cur.fetchall()
    pg_header = [column[0] for column in cur.description]

    i=1
    # iterate through each row of data
    for row in rows:
        # create dictionary for each row of data
        csv_dict = dict(zip(csv_header, csv_data[i]))       # dictionary from csv data
        pg_dict = dict(zip(pg_header, row))                 # dictionary from postgres data
        # iterate through each keys
        for key in csv_header:
            # compare values from each key
            if key=='shape':
                pg_geom = remove_whitespace(str(pg_dict.get(key)))
                csv_geom = remove_whitespace(str(csv_dict.get(key)))
                assert csv_geom == pg_geom
            elif key == 'timezone':
                pg_tz = pg_dict.get(key)
                csv_tz = convert_to_utc(csv_dict.get(key))
                assert pg_tz == csv_tz
            else:
                assert str(csv_dict.get(key)) == str(pg_dict.get(key))
        i=i+1


#compare csv data with postgres data using geopetl
def test_assert_data_2(csv_dir, postgis, table_name):
    # read staging data from csv
    with open(csv_dir, newline='') as f:
        reader = csv.reader(f)
        csv_data1 = list(reader)
    # remove object_id from csv_data for now
    csv_data = []
    for r in csv_data1:
        print(r[1:])
        csv_data.append(r[1:])

    # list of column names
    csv_header = csv_data[0]

    # read data using petl
    db_data = etl.frompostgis(dbo=postgis.dbo, table_name=table_name,fields=['textfield','datefield','numericfield','shape','timezone'])
    pg_header = db_data[0]

    i=1
    # iterate through each row of data
    for row in db_data[1:]:
        # create dictionary for each row of data using same set of keys
        etl_dict = dict(zip(pg_header, row))                # dictionary from etl data
        csv_dict = dict(zip(csv_header, csv_data[i]))       # dictionary from csv data

        # iterate through each keys
        for key in csv_header:
            # assert shape field
            if key=='shape':
                pg_geom = remove_whitespace(str(etl_dict.get(key)))
                csv_geom = remove_whitespace(str(csv_dict.get(key)))
                assert csv_geom == pg_geom
            elif key == 'timezone':
                pg_tz = etl_dict.get(key)
                csv_tz = convert_to_utc(csv_dict.get(key))
                assert pg_tz == csv_tz

            # compare values from each key
            else:
                assert str(csv_dict.get(key)) == str(etl_dict.get(key))
        i = i+1

