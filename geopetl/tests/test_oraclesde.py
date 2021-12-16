import pytest
import petl as etl
import cx_Oracle
#from geopetl.tests.db_config import oracleDBcredentials
from geopetl.oracle_sde import OracleSdeDatabase, OracleSdeTable
import csv
import os
import datetime
from dateutil import parser as dt_parser
import re
from pytz import timezone, utc
import tests_config as config
from tests_config import remove_whitespace, line_csv_dir, line_table_name, polygon_csv_dir, line_table_name,polygon_table_name,point_table_name, point_csv_dir


############################################# FIXTURES ################################################################

# return Oracl3 database object
@pytest.fixture
def oraclesde_db(host, port, service_name,user, pw):
    # create connection string
    dsn = cx_Oracle.makedsn(host, port, service_name=service_name)
    # db connection object
    connection = cx_Oracle.connect(user, pw, dsn, encoding="UTF-8")
    # create & return OracleSdeDatabase object
    dbo = OracleSdeDatabase(connection)
    return dbo


# # return csv file directory containing staging data
# @pytest.fixture
# def csv_dir():
#     csv_dir = 'geopetl/tests/fixtures_data/staging/point.csv'
#     return csv_dir
#
#
# # return table name for Oracle table based on csv file name
# @pytest.fixture
# def table_name_no_schema(csv_dir, schema):
#     head_tail = os.path.split(csv_dir)
#     # define which table based on csv name
#     if 'point' in head_tail[1]:
#         table_name = config.point_table_name
#     elif 'polygon' in head_tail[1]:
#         table_name = config.polygon_table_name
#     elif 'line' in head_tail[1]:
#         table_name = config.line_table_name
#     return table_name
#
#
# @pytest.fixture
# def table_name_with_schema(table_name_no_schema, schema):
#     return'.'.join([schema,table_name_no_schema])
#

@pytest.fixture
def csv_data():
    csv_data = etl.fromcsv(point_csv_dir).convert(['objectid','numericfield'], int)
    csv_data = etl.convert(csv_data,['timestamp','datefield','timezone'], lambda row: dt_parser.parse(row))
    return csv_data

# write csv staging data to test table using geopetl
@pytest.fixture
def create_test_tables(oraclesde_db):
    # populate a new geopetl table object with staging data from csv file
    rows = etl.fromcsv(point_csv_dir)
    # write geopetl table to Oracle
    rows.tooraclesde(oraclesde_db.dbo, point_table_name)

# fetch data from database using geopetl
@pytest.fixture
def db_data(oraclesde_db):
    db_col = etl.fromoraclesde(dbo=oraclesde_db.dbo,table_name=point_table_name)
    return db_col

@pytest.fixture
def create_test_table_noid(oraclesde_db, schema):
    csv_data = etl.fromcsv(point_csv_dir).cutout('objectid')
    csv_data.tooraclesde(oraclesde_db.dbo, '{}.{}'.format(point_table_name, schema) )

######################################   TESTS   ####################################################################

# read number of rows
def test_all_rows_written(host, port, service_name,user, pw,schema, create_test_tables): #
    # read staging data from csv
    with open(point_csv_dir, newline='') as f:
        reader = csv.reader(f)
        csv_data = list(reader)

    csv_row_count = len(csv_data[1:])

    dsn = cx_Oracle.makedsn(host, port, service_name=service_name)
    connection = cx_Oracle.connect(user, pw, dsn, encoding="UTF-8")

    try:
        with connection.cursor() as cursor:
            # execute the insert statement
            cursor.execute("select * from {}.{} ".format(schema,point_table_name))
            result = cursor.fetchall()
    except cx_Oracle.Error as error:
        print('Error occurred')
        print(error)

    # get number of rows from query
    oracle_num_rows = len(result)
    assert csv_row_count == oracle_num_rows



# compare csv data with oracle data using oraclecx
def test_assert_data(oraclesde_db, csv_data, schema):
    # list of csv column names
    csv_header = csv_data[0]

    # read data using oracle_cx
    cursor = oraclesde_db.dbo.cursor()
    cursor.execute(
        '''select objectid,textfield,numericfield,timestamp,datefield,
         to_char(timezone, 'YYYY-MM-DD HH24:MI:SS.FFTZH:TZM') as timezone,
         sde.st_astext(shape) as shape from {}.{}'''.format(schema, point_table_name))
    # list of csv column names
    db_header = [column[0] for column in cursor.description]
    rows = cursor.fetchall()

    i = 1
    # iterate through each row of data
    for row in rows:
        # create dictionary for each row of data using same set of keys
        csv_dict = dict(zip(csv_header, csv_data[i]))   # dictionary from csv data
        oracle_dict = dict(zip(db_header, row))         # dictionary from oracle data

        for key in csv_header:
            if key == 'objectid':
                 continue
            elif key == 'timezone':
                db_val = oracle_dict.get(key.upper())
                db_val = dt_parser.parse(db_val)
                csv_val = csv_dict.get(key)
                assert db_val == csv_val
            elif key == 'shape':
                db_val = remove_whitespace(str(oracle_dict.get(key.upper())))
                csv_val = remove_whitespace(str(csv_dict.get(key)))
                assert db_val == csv_val
            else:
                db_val = oracle_dict.get(key.upper())
                csv_val = csv_dict.get(key)
                assert csv_val == db_val
        i=i+1



# # compare csv data with oracle data using geopetl
def test_assert_data_2( oraclesde_db, db_data,csv_data,schema):
    tb =  oraclesde_db.table('{}.{}'.format(schema,point_table_name))
    # list of column names
    keys = csv_data[0]

    i=1
    # iterate through each row of data
    for row in db_data[1:]:
        # create dictionary for each row of data using same set of keys
        csv_dict = dict(zip(keys, csv_data[i]))         # dictionary from csv data
        oracle_dict = dict(zip(db_data[0], row))        # dictionary from Oracle data

        for key in keys:
            if key == tb.geom_field:
                pg_geom = remove_whitespace(str(oracle_dict.get(tb.geom_field)))
                csv_geom = remove_whitespace(str(csv_dict.get(tb.geom_field)))
                assert csv_geom == pg_geom
            else:
                val1 = oracle_dict.get(key)
                val2 = csv_dict.get(key)
                assert val1 == val2
        i=i+1


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
def test_with_types(db_data,oraclesde_db, schema):
    tb = oraclesde_db.table('{}.{}'.format(schema,point_table_name))
    data1 = db_data
    # load to second test table
    db_data.tooraclesde(oraclesde_db.dbo, '{}.{}'.format(schema,point_table_name)+'2')
    # extract from second test table
    data2 = etl.fromoraclesde(dbo=oraclesde_db.dbo, table_name='{}.{}'.format(schema,point_table_name) + '2')

    i = 1
    # iterate through each row of data
    for row in data1[1:]:
        # create dictionary for each row of data using same set of keys
        db_dict1 = dict(zip(data1[0], data1[i]))  # dictionary from etl data
        db_dict2 = dict(zip(data2[0], data2[i]))  # dictionary from csv data

        # iterate through each row of DB data
        for key in db_data[0]:
            # assert shape field
            if key == tb.geom_field:
                db_geom1 = remove_whitespace(str(db_dict1.get(key)))
                db_geom2 = remove_whitespace(str(db_dict2.get(key)))
                assert db_geom1 == db_geom2
            # assert values from each key
            else:
                assert db_dict1.get(key) == db_dict2.get(key)
        i = i + 1

# load csv data to oracle db without an objectid field using geopetl and assert data
def test_assert_data_no_id(create_test_table_noid,csv_data,schema, db_data,oraclesde_db):
    tb = oraclesde_db.table('{}.{}'.format(schema,point_table_name))
    i=1
    # iterate through each row of data
    for row in db_data[1:]:
        # create dictionary with header and each row of data
        csv_dict = dict(zip(csv_data[0], csv_data[i]))          # dictionary from csv data
        oracle_dict = dict(zip(db_data[0], row))                # dictionary from db data

        for key in db_data[0]:
            # assert objectid is not null
            if key == 'objectid' and 'objectid' in csv_data[0] and 'objectid' in db_data[0]:
                assert (oracle_dict.get('objectid') is not None)
            elif key == tb.geom_field:
                pg_geom = remove_whitespace(str(oracle_dict.get(tb.geom_field)))
                csv_geom = remove_whitespace(str(csv_dict.get(tb.geom_field)))
                assert csv_geom == pg_geom
            # assert values from each key
            else:
                assert oracle_dict.get(key) == csv_dict.get(key)
        i=i+1

# assert data by loading and extracting data without providing schema
def test_without_schema( oraclesde_db, csv_data):
    tb = oraclesde_db.table(point_table_name)
    etl.tooraclesde(csv_data, oraclesde_db.dbo, point_table_name)
    data = etl.fromoraclesde(dbo=oraclesde_db.dbo, table_name=point_table_name)

    for row in data[0]:
        # list of column names
        keys = csv_data[0]
        i = 1
        # iterate through each row of data
        for row in data[1:]:
            # create dictionary for each row of data using same set of keys
            etl_dict = dict(zip(data[0], row))          # dictionary from etl data
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
            i = i + 1


def test_dsn_connection(host,service_name,csv_data, oraclesde_db, db_data,schema,pw, user):
    tb = oraclesde_db.table('{}.{}'.format(schema, point_table_name))
    dsn = cx_Oracle.makedsn(host, 1521, service_name=service_name)
    connection = cx_Oracle.connect(user=user, password=pw, dsn=dsn,
                                   encoding="UTF-8")
    etl.tooraclesde(csv_data, dsn, '{}.{}'.format(schema, point_table_name))
    #csv_data.tooraclesde(dsn, table_name_with_schema)
    for row in db_data[0]:
        # list of column names
        keys = csv_data[0]
        i = 1
        # iterate through each row of data
        for row in db_data[1:]:
            # create dictionary for each row of data using same set of keys
            etl_dict = dict(zip(db_data[0], row))  # dictionary from etl data
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
def test_line_assertion( oraclesde_db, schema):
    tb = oraclesde_db.table('{}.{}'.format(schema, line_table_name))
    rows = etl.fromcsv(line_csv_dir)
    rows.tooraclesde(oraclesde_db.dbo, '{}.{}'.format(schema,line_table_name))
    csv_data = etl.fromcsv(line_csv_dir).convert(['objectid'], int)
    # list of column names
    keys = csv_data[0]

    # read data from test DB using petl
    db_data = etl.fromoraclesde(dbo=oraclesde_db.dbo, table_name='{}.{}'.format(schema,line_table_name))

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


def test_polygon_assertion(oraclesde_db, csv_data, schema):
    tb = oraclesde_db.table('{}.{}'.format(schema, polygon_table_name))
    rows = etl.fromcsv(polygon_csv_dir)
    rows.tooraclesde(oraclesde_db.dbo, '{}.{}'.format(schema,polygon_table_name))
    csv_data = etl.fromcsv(polygon_csv_dir).convert(['objectid'], int)

    # list of column names
    keys = csv_data[0]

    # read data from test DB using petl
    db_data = etl.fromoraclesde(dbo=oraclesde_db.dbo, table_name='{}.{}'.format(schema, polygon_table_name))

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