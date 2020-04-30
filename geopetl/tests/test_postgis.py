import pytest
import petl as etl
from geopetl.postgis import PostgisDatabase
import psycopg2
#from geopetl.tests.db_config import postgis_creds
import csv
import os
import re

def remove_whitespace(stringval):
    shapestring = str(stringval)
    geom_type = re.findall("[A-Z]{1,12}", shapestring)[0]
    coordinates = re.findall("\d+\.\d+", shapestring)
    coordinates= [str(float(coords))for coords in coordinates]

    if geom_type == 'point' or geom_type=="POINT":
        geom = "{type}({x})".format(type=geom_type, x=" ".join(coordinates))
    elif geom_type == 'polygon' or geom_type=="POLYGON":
        geom = "{type}(({x}))".format(type=geom_type, x=" ".join(coordinates))
    return geom
############################################# FIXTURES ################################################################

# return postgis database object
@pytest.fixture
def postgis(db, user, pw):
    # create connection string
    # dsn = "host='localhost' dbname={my_database} user={user} password={passwd}".format(my_database=postgis_creds['dbname'],
    #                                                                                    user=postgis_creds['user'],
    #                                                                                    passwd=postgis_creds['pw'])
    dsn = "host={my_host} dbname={my_database} user={user} password={passwd}".format(my_host='localhost',
                                                                                        my_database=db,
                                                                                        user= user,
                                                                                        passwd=pw)
    # create & return geopetl postgis object
    postgis_db = PostgisDatabase(dsn)
    return postgis_db


# return csv file directory containing staging data
@pytest.fixture
def csv_dir():
    csv_dir = '~/work/geopetl/geopetl/tests/fixtures_data/staging/point.csv'
    return csv_dir


# return table name for postgis table based on json file name
@pytest.fixture
def table_name(csv_dir):
    head_tail = os.path.split(csv_dir)
    # define which table based on schema file name
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
def create_test_tables(postgis, table_name, csv_dir):
    # populate a new geopetl table object with staging data from csv file
    rows = etl.fromcsv(csv_dir)
    # write geopetl table to postgis
    rows.topostgis(postgis.dbo, table_name)



######################################   TESTS   ####################################################################

# read number of rows
def test_all_rows_written(db, user, pw, csv_dir,create_test_tables,table_name): #
    # read staging data from csv
    with open(csv_dir, newline='') as f:
        reader = csv.reader(f)
        csv_data = list(reader)
    csv_row_count = len(csv_data[1:])

    # connect to postgis DB using psycopg2
    connection = psycopg2.connect(user=user,
                                  password=pw,
                                  database=db)
    cur = connection.cursor()
    # query all data from postgis table
    cur.execute('Select * from public.' + table_name)
    result = cur.fetchall()

    # get number of rows from query
    postgis_num_rows = len(result)
    assert csv_row_count == postgis_num_rows


# compare csv data with postgres data using psycopg2
def test_assert_data(csv_dir, postgis, table_name):
    # read staging data from csv
    with open(csv_dir, newline='') as f:
        reader = csv.reader(f)
        csv_data = list(reader)
    # list of column names
    keys = csv_data[0]

    # read data using postgis
    cur = postgis.dbo.cursor()
    cur.execute('select objectid,textfield,datefield,numericfield,st_astext(shape) from ' + table_name)
    rows = cur.fetchall()

    i=1
    # iterate through each row of data
    for row in rows:
        # create dictionary for each row of data using same set of keys
        csv_dict = dict(zip(keys, csv_data[i]))     # dictionary from csv data
        pg_dict = dict(zip(keys, row))              # dictionary from postgis data
        # iterate through each keys
        for key in keys:
            # compare values from each key
            if key=='shape':
                pg_geom = remove_whitespace(str(pg_dict.get('shape')))
                csv_geom = remove_whitespace(str(csv_dict.get('shape')))
                assert csv_geom == pg_geom
            else:
                assert str(csv_dict.get(key)) == str(pg_dict.get(key))
        i=i+1


#compare csv data with postgres data using geopetl
def test_assert_data_2(csv_dir, postgis, table_name):
    # read staging data from csv
    with open(csv_dir, newline='') as f:
        reader = csv.reader(f)
        csv_data = list(reader)

    # list of column names
    keys = csv_data[0]

    # read data using petl
    db_data = etl.frompostgis(dbo=postgis.dbo, table_name=table_name)

    i=1
    # iterate through each row of data
    for row in db_data[1:]:
        # create dictionary for each row of data using same set of keys
        etl_dict = dict(zip(keys, row))          # dictionary from etl data
        csv_dict = dict(zip(keys, csv_data[i]))  # dictionary from csv data
        # iterate through each keys
        for key in keys:
            if key=='shape':
                pg_geom = remove_whitespace(str(etl_dict.get('shape')))
                csv_geom = remove_whitespace(str(csv_dict.get('shape')))
                assert csv_geom == pg_geom
            # compare values from each key
            else:
                assert str(csv_dict.get(key)) == str(etl_dict.get(key))
        i = i+1
