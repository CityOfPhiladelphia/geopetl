import pytest
import petl as etl
from geopetl.postgis import PostgisDatabase, PostgisTable
import psycopg2
import csv
import os
from pytz import timezone
from dateutil import parser as dt_parser
from tests_config import remove_whitespace, line_csv_dir, line_table_name, polygon_csv_dir, line_table_name,polygon_table_name,point_table_name


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
def table_name_no_schema(csv_dir):
    head_tail = os.path.split(csv_dir)
    # define which table based on csv name
    if 'point' in head_tail[1]:
        table_name = point_table_name
    elif 'polygon' in head_tail[1]:
        table_name = polygon_table_name
    elif 'line' in head_tail[1]:
        table_name = line_table_name
    return table_name

# @pytest.fixture
# def table_name_with_schema(table_name_no_schema, schema):
#     return'.'.join([schema,table_name_no_schema])


# create new table and write csv staging data to it
@pytest.fixture
def create_test_tables(postgis, csv_dir, column_definition,schema):
    # populate a new geopetl table object with staging data from csv file
    rows = etl.fromcsv(csv_dir)
    # write geopetl table to postgis
    rows.topostgis(postgis.dbo, '{}.{}'.format(schema,point_table_name), column_definition_json=column_definition, from_srid=2272)

@pytest.fixture
def csv_data(csv_dir):
    csv_data = etl.fromcsv(csv_dir).convert(['objectid','numericfield'], int)
    csv_data = etl.convert(csv_data,['timestamp','datefield','timezone'], lambda row: dt_parser.parse(row))
    csv_data = etl.convert(csv_data, 'datefield', lambda row: row.date())
    return csv_data


@pytest.fixture
def db_data(postgis,schema):
    db_col = etl.frompostgis(dbo=postgis.dbo,table_name='{}.{}'.format(schema,point_table_name))
    return db_col

######################################   TESTS   ####################################################################

# read number of rows
def test_all_rows_written(db, user, host, pw, csv_dir,create_test_tables,csv_data,schema):

    csv_row_count = etl.nrows(csv_data)
    etl.nrows(csv_data)

    # connect to postgis DB using psycopg2
    connection = psycopg2.connect(user=user,
                                  host=host,
                                  password=pw,
                                  database=db)
    cur = connection.cursor()
    # query all data from postgis table
    cur.execute('Select * from {table}'.format(table='{}.{}'.format(schema,point_table_name)))
    result = cur.fetchall()

    # get number of rows from query
    postgis_num_rows = len(result)
    assert csv_row_count == postgis_num_rows


# compare csv data with postgres data using psycopg2
def test_assert_data(csv_dir, postgis, csv_data,schema):
    # list of column names
    keys = csv_data[0]
    # read data using postgis
    cursor = postgis.dbo.cursor()
    cursor.execute('select objectid,textfield,timestamp,numericfield, timezone, st_astext(shape) as shape, datefield from ' + '{}.{}'.format(schema,point_table_name))
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
                assert csv_dict.get(key) == pg_dict.get(key)
        i=i+1


# #compare csv data with postgres data using geopetl
def test_assert_data_2(csv_dir, postgis, schema, csv_data):
    tb = postgis.table('{}.{}'.format(schema, point_table_name))
    # list of column names
    keys = csv_data[0]

    # read data from test DB using petl
    db_data = etl.frompostgis(dbo=postgis.dbo, table_name='{}.{}'.format(schema, point_table_name))

    i=1
    # iterate through each row of data
    for row in db_data[1:]:
        # create dictionary for each row of data using same set of keys
        etl_dict = dict(zip(db_data[0], row))       # dictionary from etl data
        csv_dict = dict(zip(keys, csv_data[i]))     # dictionary from csv data
        # iterate through each keys
        for key in keys:
            # assert shape field
            if key== tb.geom_field:
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
    db_col = db_data[key]
    for i in range(len(db_col)):
         assert db_col[i] == csv_col[i]


def test_assert_numericfield(csv_data, db_data):
    key = 'numericfield'
    csv_col = csv_data[key]
    db_col = db_data[key]
    for i in range(len(db_col)):
         assert db_col[i] == csv_col[i]

def test_assert_datefield(csv_data, db_data):
    key = 'datefield'
    csv_col = csv_data[key]
    db_col = db_data[key]
    for i in range(len(db_col)):
         assert db_col[i] == csv_col[i]

def test_assert_shape(csv_data, db_data):
    key = 'shape'
    csv_col = csv_data[key]
    db_col = db_data[key]
    for i in range(len(db_col)):
        db_val = remove_whitespace(str(db_col[i]))
        csv_val = remove_whitespace(str(csv_col[i]))
        assert db_val == csv_val


def test_assert_textfield(csv_data, db_data):
    key = 'textfield'
    csv_col = csv_data[key]
    db_col = db_data[key]
    for i in range(len(db_col)):
         assert db_col[i] == csv_col[i]

def test_assert_timezone(csv_data, db_data):
    key = 'timezone'
    csv_col = csv_data[key]
    db_col = db_data[key]
    for i in range(len(db_col)):
         assert db_col[i] == csv_col[i]

# assert DB data with itself
def test_with_types(db_data, schema, postgis,column_definition):
    tb = postgis.table('{}.{}'.format(schema, point_table_name))
    # read data from DB
    data1 = db_data
    #load to second test table
    etl.topostgis(db_data, postgis.dbo, '{}.{}'.format(schema, point_table_name)+'2', from_srid=2272, column_definition_json=column_definition)
    #extract from second test table
    data2 = etl.frompostgis(dbo=postgis.dbo,table_name='{}.{}'.format(schema, point_table_name)+'2')

    i = 1
    # iterate through each row of DB data and assert data from the 2 test tables
    for row in db_data[1:]:
        # create dictionary with header and each row of data
        db_dict1 = dict(zip(data1[0], data1[i]))  # dictionary from db data
        db_dict2 = dict(zip(data2[0], data2[i]))  # dictionary from db data

        # iterate through each key in header row
        for key in db_data[0]:
            # assert shape field
            if key == tb.geom_field:
                geom1 = remove_whitespace(str(db_dict1.get(key)))
                geom2 = remove_whitespace(str(db_dict2.get(key)))
                assert geom1 == geom2
            # assert values from each key
            else:
                assert db_dict1.get(key) == db_dict2.get(key)
        i = i + 1

# assert data by loading and extracting data without providing schema
# def test_without_schema(db_data, postgis, column_definition,csv_data, table_name_no_schema):
#     etl.topostgis(csv_data, postgis.dbo, table_name_no_schema, from_srid=2272, column_definition_json=column_definition)
#     data = etl.frompostgis(dbo=postgis.dbo, table_name=table_name_no_schema)
#
#     for row in data[0]:
#         # list of column names
#         keys = csv_data[0]
#         i = 1
#         # iterate through each row of data
#         for row in data[1:]:
#             # create dictionary for each row of data using same set of keys
#             etl_dict = dict(zip(data[0], row))          # dictionary from etl data
#             csv_dict = dict(zip(keys, csv_data[i]))     # dictionary from csv data
#             # iterate through each keys
#             for key in keys:
#                 # assert shape field
#                 if key == 'shape':
#                     pg_geom = remove_whitespace(str(etl_dict.get(key)))
#                     csv_geom = remove_whitespace(str(csv_dict.get(key)))
#                     assert csv_geom == pg_geom
#                 # compare values from each key
#                 else:
#                     assert csv_dict.get(key) == etl_dict.get(key)
#             i = i + 1

# write using a string connection to db
def test_dsn_connection(csv_data,db, user, pw, host,postgis, column_definition, schema):
    tb = postgis.table('{}.{}'.format(schema, point_table_name))
    my_dsn = '''dbname={db} user={user} password={pw} host={host}'''.format(db=db,user=user,pw=pw,host=host)
    etl.topostgis(csv_data, my_dsn,
                  '{}.{}'.format(schema, point_table_name), from_srid=2272, column_definition_json=column_definition)
    data = etl.frompostgis(dbo=postgis.dbo, table_name='{}.{}'.format(schema, point_table_name))
    for row in data[0]:
        # list of column names
        keys = csv_data[0]
        i = 1
        # iterate through each row of data
        for row in data[1:]:
            # create dictionary for each row of data using same set of keys
            etl_dict = dict(zip(data[0], row))  # dictionary from etl data
            csv_dict = dict(zip(keys, csv_data[i]))  # dictionary from csv data
            # iterate through each keys
            for key in keys:
                # assert shape field
                if key == tb.geom_field:
                    pg_geom = remove_whitespace(str(etl_dict.get(key)))
                    csv_geom = remove_whitespace(str(csv_dict.get(key)))
                    assert csv_geom == pg_geom
                # compare values from each key
                else:
                    assert csv_dict.get(key) == etl_dict.get(key)
            i = i + 1


#compare csv data with postgres data using geopetl
def test_line_assertion(csv_dir, postgis, csv_data, schema):

    tb = postgis.table('{}.{}'.format(schema,line_table_name))
    rows = etl.fromcsv(line_csv_dir)
    rows.topostgis(postgis.dbo, '{}.{}'.format(schema,line_table_name), from_srid=2272)
    csv_data = etl.fromcsv(line_csv_dir).convert(['objectid'], int)
    # list of column names
    keys = csv_data[0]

    # read data from test DB using petl
    db_data = etl.frompostgis(dbo=postgis.dbo, table_name='{}.{}'.format(schema,line_table_name))

    i=1
    # iterate through each row of data
    for row in db_data[1:]:
        # create dictionary for each row of data using same set of keys
        etl_dict = dict(zip(db_data[0], row))       # dictionary from etl data
        csv_dict = dict(zip(keys, csv_data[i]))     # dictionary from csv data
        # iterate through each keys
        for key in keys:
            # assert shape field
            if key== tb.geom_field:
                pg_geom = remove_whitespace(str(etl_dict.get(key)))
                csv_geom = remove_whitespace(str(csv_dict.get(key)))
                assert csv_geom == pg_geom
            # compare values from each key
            else:
                assert csv_dict.get(key) == etl_dict.get(key)
        i = i+1



#compare csv data with postgres data using geopetl
def test_polygon_assertion(postgis,schema):
    # geopetl table object
    tb = postgis.table('{}.{}'.format(schema,polygon_table_name))

    rows = etl.fromcsv(polygon_csv_dir)
    rows.topostgis(postgis.dbo, '{}.{}'.format(schema,polygon_table_name), from_srid=2272)
    csv_data = etl.fromcsv(polygon_csv_dir).convert(['objectid'], int)

    # list of column names
    keys = csv_data[0]
    # read data from test DB using petl
    db_data = etl.frompostgis(dbo=postgis.dbo, table_name='{}.{}'.format(schema,polygon_table_name))
    i=1

    # iterate through each row of data
    for row in db_data[1:]:
        # create dictionary for each row of data using same set of keys
        etl_dict = dict(zip(db_data[0], row))       # dictionary from etl data
        csv_dict = dict(zip(keys, csv_data[i]))     # dictionary from csv data
        # iterate through each keys
        for key in keys:
            # assert shape field
            if key == tb.geom_field:
                pg_geom = remove_whitespace(str(etl_dict.get(key)))
                csv_geom = remove_whitespace(str(csv_dict.get(key)))
                assert csv_geom == pg_geom
            # compare values from each key
            else:
                assert csv_dict.get(key) == etl_dict.get(key)
        i = i+1