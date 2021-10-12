import pytest
import petl as etl
from geopetl.postgis import PostgisDatabase
import psycopg2
from pytz import timezone
import csv
import os
import re
from dateutil import parser as dt_parser

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

@pytest.fixture
def csv_data(csv_dir):
    csv_data = etl.fromcsv(csv_dir).convert(['objectid','numericfield'], int)
    csv_data = etl.convert(csv_data,['timestamp','datefield','timezone'], lambda row: dt_parser.parse(row))
    csv_data = etl.convert(csv_data, 'datefield', lambda row: row.date())
    return csv_data


@pytest.fixture
def db_data(postgis, table_name):
    db_col = etl.frompostgis(dbo=postgis.dbo,table_name=table_name)
    #db_data = etl.frompostgis(dbo=postgis.dbo, table_name=table_name,fields=['textfield','datefield','timestamp','numericfield','shape','timezone'])
    return db_col

######################################   TESTS   ####################################################################

# compare number of rows
def test_all_rows_written(db, user, host, pw, csv_dir,create_test_tables,table_name): #
    # read staging data from csv
    with open(csv_dir, newline='') as f:
        reader = csv.reader(f)
        csv_data = list(reader)

    # connect to postgis DB using psycopg2
    connection = psycopg2.connect(user=user,
                                  host=host,
                                  password=pw,
                                  database=db)
    cur = connection.cursor()
    # query all data from postgis table
    cur.execute('Select * from {table}'.format(table= table_name))
    result = cur.fetchall()

    # assert number of rows written to pg with test data
    assert len(csv_data[1:]) == len(result)


# compare csv data with postgres data using psycopg2
def test_assert_data(csv_dir, postgis, table_name):
    # read staging data from csv
    csv_data = etl.fromcsv(csv_dir).convert(['objectid','numericfield'], int)
    csv_data = etl.convert(csv_data,['timestamp','datefield','timezone'], lambda row: dt_parser.parse(row)) 
    csv_data = etl.convert(csv_data,'datefield', lambda row: row.date())
    csv_header = csv_data[0]

    # read data using postgis
    cur = postgis.dbo.cursor()
    cur.execute('select textfield,timestamp,numericfield, timezone, st_astext(shape) as shape, datefield from ' + table_name)
    db_data = cur.fetchall()
    pg_header = [column[0] for column in cur.description]

    i=1
    # iterate through each row of data
    for row in db_data:
        # create dictionary for each row of data
        csv_dict = dict(zip(csv_header, csv_data[i]))       # dictionary from csv data
        pg_dict = dict(zip(pg_header, row))                 # dictionary from postgres data
        # iterate through each keys
        for key in pg_header:
            # compare values from each key
            if key=='shape':
                pg_geom = remove_whitespace(str(pg_dict.get(key)))
                csv_geom = remove_whitespace(str(csv_dict.get(key)))
                assert csv_geom == pg_geom
            else:
                print(csv_dict.get(key))
                print(pg_dict.get(key))
                assert csv_dict.get(key) == pg_dict.get(key)

        i=i+1


#compare csv data with postgres data using geopetl
def test_assert_data_2(csv_dir, postgis, table_name):
    # read staging data from csv using petl
    csv_data = etl.fromcsv(csv_dir).convert(['objectid','numericfield'], int)
    csv_data = etl.convert(csv_data,['timestamp','datefield','timezone'], lambda row: dt_parser.parse(row)) #.replace(microsecond=0))
    csv_data = etl.convert(csv_data, 'datefield', lambda row: row.date())
    csv_header = csv_data[0]

    # read data using geopetl
    db_data = etl.frompostgis(dbo=postgis.dbo, table_name=table_name,fields=['textfield','datefield','timestamp','numericfield','shape','timezone'])
    pg_header = db_data[0]

    i=1
    # iterate through each row of data
    for row in db_data[1:]:
        # create dictionary for each row of data using same set of keys
        etl_dict = dict(zip(pg_header, row))                # dictionary from etl data
        csv_dict = dict(zip(csv_header, csv_data[i]))       # dictionary from csv data

        # iterate through each keys
        for key in etl_dict:
            # assert shape field
            if key=='shape':
                pg_geom = remove_whitespace(str(etl_dict.get(key)))
                csv_geom = remove_whitespace(str(csv_dict.get(key)))
                assert csv_geom == pg_geom
            # compare values from each key
            else:
                assert csv_dict.get(key) == etl_dict.get(key)
        i = i+1

def test_assert_timestamp(csv_data, db_data):
    key = 'timestamp'
    csv_col = csv_data[key]
    # get oracle data
    db_col = db_data[key]
    for i in range(len(db_col)):
         assert db_col[i] == csv_col[i]


def test_assert_numericfield(csv_data, db_data):
    key = 'numericfield'
    csv_col = csv_data[key]
    # get oracle data
    db_col = db_data[key]
    for i in range(len(db_col)):
         assert db_col[i] == csv_col[i]

def test_assert_datefield(csv_data, db_data):
    key = 'datefield'
    csv_col = csv_data[key]
    # get oracle data
    db_col = db_data[key]
    for i in range(len(db_col)):
         assert db_col[i] == csv_col[i]

def test_assert_shape(csv_data, db_data):
    key = 'shape'
    csv_col = csv_data[key]
    # get oracle data
    db_col = db_data[key]
    for i in range(len(db_col)):
        db_val = remove_whitespace(str(db_col[i]))
        csv_val = remove_whitespace(str(csv_col[i]))
        assert db_val == csv_val


def test_assert_textfield(csv_data, db_data):
    key = 'textfield'
    csv_col = csv_data[key]
    # get oracle data
    db_col = db_data[key]
    for i in range(len(db_col)):
         assert db_col[i] == csv_col[i]

def test_assert_timezone(csv_data, db_data):
    key = 'timezone'
    csv_col = csv_data[key]
    # get oracle data
    db_col = db_data[key]
    for i in range(len(db_col)):
         assert db_col[i] == csv_col[i]

