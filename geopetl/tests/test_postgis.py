import pytest
import petl as etl
from geopetl.postgis import PostgisDatabase
import psycopg2
import csv
import os
import re
from pytz import timezone
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
    # define which table based on csv name
    table = ''
    if 'point' in head_tail[1]:
        table = 'point'
    elif 'polygon' in head_tail[1]:
        table = 'polygon'
    # define table name
    table_name = table + '_table'
    return table_name


# create new table and write csv staging data to it
@pytest.fixture
def create_test_tables(postgis, table_name, csv_dir, column_definition):
    # populate a new geopetl table object with staging data from csv file
    rows = etl.fromcsv(csv_dir)
    # write geopetl table to postgis
    rows.topostgis(postgis.dbo, table_name, column_definition_json=column_definition, from_srid=2272)



######################################   TESTS   ####################################################################

# read number of rows
def test_all_rows_written(db, user, host, pw, csv_dir,create_test_tables,table_name, schema): #
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
def test_assert_data(csv_dir, postgis, table_name, schema):
    # read staging data from csv
    csv_data = etl.fromcsv(csv_dir).convert(['objectid', 'numericfield'], int)
    csv_data = etl.convert(csv_data, ['timestamp', 'datefield','timezone'],lambda row: dt_parser.parse(row))
    csv_data = etl.convert(csv_data, 'datefield', lambda row: row.date())
    #csv_data = etl.convert(csv_data, 'timezone', lambda row: dt_parser.parse(row))

    # list of column names
    keys = csv_data[0]

    # read data using postgis
    cursor = postgis.dbo.cursor()
    cursor.execute('select objectid,textfield,timestamp,numericfield, timezone, st_astext(shape) as shape, datefield from ' + table_name)
    rows = cursor.fetchall()
    header = [column[0] for column in cursor.description]

    i=1
    # iterate through each row of data
    for row in rows:
        # create dictionary for each row of data using same set of keys
        csv_dict = dict(zip(keys, csv_data[i]))     # dictionary from csv data
        pg_dict = dict(zip(header, row))            # dictionary from postgis data
        # iterate through each keys
        for key in keys:
            # compare values from each key
            if key=='shape':
                pg_geom = remove_whitespace(str(pg_dict.get(key)))
                csv_geom = remove_whitespace(str(csv_dict.get(key)))
                assert csv_geom == pg_geom
            else:
                # a = csv_dict.get(key)
                # b = pg_dict.get(key)
                # print('a ',a)
                # print('b ',b)
                assert csv_dict.get(key) == pg_dict.get(key)
        i=i+1


#compare csv data with postgres data using geopetl
def test_assert_data_2(csv_dir, postgis, table_name):
    # read staging data from csv using geopetl
    csv_data = etl.fromcsv(csv_dir).convert(['objectid','numericfield'], int)
    csv_data = etl.convert(csv_data,['timestamp','datefield','timezone'], lambda row: dt_parser.parse(row))
    csv_data = etl.convert(csv_data, 'datefield', lambda row: row.date())
    # list of column names
    keys = csv_data[0]

    # read data from test DB using petl
    db_data = etl.frompostgis(dbo=postgis.dbo, table_name=table_name)

    i=1
    # iterate through each row of data
    for row in db_data[1:]:
        # create dictionary for each row of data using same set of keys
        etl_dict = dict(zip(db_data[0], row))       # dictionary from etl data
        csv_dict = dict(zip(keys, csv_data[i]))     # dictionary from csv data

        # iterate through each keys
        for key in keys:
            # assert shape field
            if key=='shape':
                pg_geom = remove_whitespace(str(etl_dict.get(key)))
                csv_geom = remove_whitespace(str(csv_dict.get(key)))
                assert csv_geom == pg_geom
            # compare values from each key
            else:
                # a = csv_dict.get(key)
                # b = etl_dict.get(key)
                # print('a ',a)
                # print('b ',b)
                assert csv_dict.get(key) == etl_dict.get(key)
        i = i+1

