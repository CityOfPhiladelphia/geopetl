import pytest
import petl as etl
from geopetl.postgis import PostgisDatabase
import psycopg2
from pytz import timezone
import csv
import os
import re

def remove_whitespace(stringval):
    print('stringval ',stringval)
    shapestring = str(stringval)
    geom_type = re.findall("[A-Z]{1,12}", shapestring)[0]
    print('geom_type ',geom_type)
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
        print('parsed!!')
        print(this_date)
        this_date = this_date.astimezone(eastern)
        print('localized the data EST ', utc_dt)
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
    #csv_dir = 'geopetl/tests/fixtures_data/staging/point.csv'
    csv_dir = 'geopetl/tests/fixtures_data/staging/some_new.csv'
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
    table_name = 'test_user.point_table'
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
    with open(csv_dir, newline='') as f:
        reader = csv.reader(f)
        csv_data1 = list(reader)

    csv_data =[]
    print('iteration ')
    for r in csv_data1:
        print(r[1:])
        csv_data.append(r[1:])
    #csv_data = csv_data[:,1:]
    # list of column names
    keys = csv_data[0]
    print('csv_data')
    print(csv_data)
    print('keys')
    print(keys)

    # read data using postgis
    cur = postgis.dbo.cursor()
    cur.execute('select textfield,datefield,numericfield,st_astext(shape) as shape, timezone from {table}'.format(table= table_name))
    rows = cur.fetchall()
    header = [column[0] for column in cur.description]
    print('header ',header)
    i=1
    # iterate through each row of data
    for row in rows:
        print('row ',row)
        print('csv_data[i] ',csv_data[i])
        # create dictionary for each row of data using same set of keys
        csv_dict = dict(zip(keys, csv_data[i]))     # dictionary from csv data
        pg_dict = dict(zip(header, row))              # dictionary from postgis data
        # iterate through each keys
        for key in keys:
            # compare values from each key
            print('this key', key)
            if key=='shape':
                print('pg_dict.get(shape) ',pg_dict.get('shape'))
                print('csv_dict.get(shape) ',csv_dict.get('shape'))
                pg_geom = remove_whitespace(str(pg_dict.get('shape')))
                csv_geom = remove_whitespace(str(csv_dict.get('shape')))
                assert csv_geom == pg_geom
            elif key == 'timezone':
                print('#############################################################################################################')
                print('asserting timezone')
                pg_tz = pg_dict.get('timezone')
                csv_tz = convert_to_utc(csv_dict.get('timezone'))
            else:
                print('csv_dict.get(key) ',csv_dict.get(key))
                print('pg_dict.get(key) ',pg_dict.get(key))
                assert str(csv_dict.get(key)) == str(pg_dict.get(key))
        i=i+1


#compare csv data with postgres data using geopetl
def test_assert_data_2(csv_dir, postgis, table_name):
    # read staging data from csv
    with open(csv_dir, newline='') as f:
        reader = csv.reader(f)
        csv_data1 = list(reader)

    csv_data = []
    print('iteration ')
    for r in csv_data1:
        print(r[1:])
        csv_data.append(r[1:])
    # list of column names
    keys = csv_data[0]
    # read data using petl
    db_data = etl.frompostgis(dbo=postgis.dbo, table_name=table_name,fields=['textfield','datefield','numericfield','shape','timezone'])
    header = db_data[0]
    print('header ', header)
    i=1
    # iterate through each row of data
    for row in db_data[1:]:
        # create dictionary for each row of data using same set of keys
        etl_dict = dict(zip(header, row))          # dictionary from etl data
        csv_dict = dict(zip(keys, csv_data[i]))  # dictionary from csv data

        # iterate through each keys
        for key in keys:
            # assert shape field
            if key=='shape':
                pg_geom = remove_whitespace(str(etl_dict.get('shape')))
                csv_geom = remove_whitespace(str(csv_dict.get('shape')))
                assert csv_geom == pg_geom
            elif key == 'timezone':
                print('#############################################################################################################')
                print('asserting timezone')
                pg_tz = etl_dict.get('timezone')
                csv_tz = convert_to_utc(csv_dict.get('timezone'))

            # compare values from each key
            else:
                assert str(csv_dict.get(key)) == str(etl_dict.get(key))
        i = i+1

