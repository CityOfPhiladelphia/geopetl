import pytest
import petl as etl
from geopetl.postgis import PostgisDatabase
import psycopg2
from geopetl.tests.db_config import postgis_creds
import csv
import os


############################################# FIXTURES ################################################################

table_name = ''

# return postgis database object
@pytest.fixture
def postgis():
    # create connection string
    dsn = "host='localhost' dbname={my_database} user={user} password={passwd}".format(my_database=postgis_creds['dbname'],
                                                                                       user=postgis_creds['user'],
                                                                                       passwd=postgis_creds['pw'])
    # create & return geopetl postgis object
    postgis_db = PostgisDatabase(dsn)
    return postgis_db


# return .json schema file directory
@pytest.fixture
def schema_dir():
    schema_dir = 'C:\\projects\\geopetl\\geopetl\\tests\\fixtures_data\\schemas\\point3.json'
    return schema_dir


# return table name for postgis table based on json file name
@pytest.fixture
def table_name(schema_dir):
    head_tail = os.path.split(schema_dir)
    # define table based on schema file name
    table = ''
    if 'point' in head_tail[1]:
        table = 'point'
    elif 'polygon' in head_tail[1]:
        table = 'polygon'
    # define table
    table_name = table + '_table3'
    return table_name


# return csv directory based on .json
@pytest.fixture
def csv_dir(schema_dir):
    # split schema directory
    head_tail = os.path.split(schema_dir)
    table=''

    # define table based on schame file name
    if 'point' in head_tail[1]:
        table = 'point'
    elif 'polygon' in head_tail[1]:
        table = 'polygon'

    csv_dir = 'C:\\projects\\geopetl\\geopetl\\tests\\fixtures_data\\staging\\new_' + table + '.csv'
    return csv_dir


# create postgisTable object
@pytest.fixture
def create_test_tables(postgis, schema_dir, table_name,csv_dir):
    # get fields from schmema file
    #myfields = get_fields(schema_dir)
    # create a new postgis table with the fields from schema file
    #postgis.create_table(table_name, myfields)
    postgis.create_table2(schema_dir, table_name)


    # populate a new geopetl table object with staging data from csv file
    rows = etl.fromcsv(csv_dir)
    # write geopetl table to postgis
    rows.topostgis(postgis.dbo, 'public.' + table_name)
    #return postgis.table


# create_table(self, name, cols):



######################################   TESTS   ####################################################################

# read number of rows
def test_all_rows_written(csv_dir,create_test_tables,table_name): #
    # read staging data from csv
    with open(csv_dir, newline='') as f:
        reader = csv.reader(f)
        csv_data = list(reader)
    csv_row_count = len(csv_data[1:])

    # connect to postgis DB using psycopg2
    connection = psycopg2.connect(user=postgis_creds['user'],
                                  password=postgis_creds['pw'],
                                  database=postgis_creds['pw'])
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
            assert str(csv_dict.get(key)) == str(pg_dict.get(key))
        i=i+1


# compare csv data with postgres data using geopetl
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
            # compare values from each key
            assert str(csv_dict.get(key)) == str(etl_dict.get(key))
        i = i+1



# def test_extract_shema_method(postgis):
