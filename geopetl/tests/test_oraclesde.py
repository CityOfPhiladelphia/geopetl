import pytest
import petl as etl
import cx_Oracle
#from geopetl.tests.db_config import oracleDBcredentials
from geopetl.oracle_sde import OracleSdeDatabase, OracleSdeTable
import csv
import os
import datetime
from dateutil import parser as dt_parser
from pytz import timezone
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

# return oracle database object
@pytest.fixture
def oraclesde_db(host, port, service_name,user, pw):
    # create connection string
    dsn = cx_Oracle.makedsn(host, port, service_name=service_name)
    # db connection object
    connection = cx_Oracle.connect(user, pw, dsn, encoding="UTF-8")
    # create & return OracleSdeDatabase object
    dbo = OracleSdeDatabase(connection)
    return dbo


# return csv file directory containing staging data
@pytest.fixture
def csv_dir():
    #csv_dir = 'C:\\projects\\geopetl\\geopetl\\tests\\fixtures_data\\staging\\point.csv'
    #csv_dir = 'C:\\projects\\geopetl\\geopetl\\tests\\fixtures_data\\staging\\point.csv'
    #csv_dir = '~/work/geopetl/geopetl/tests/fixtures_data/staging/point.csv'
    csv_dir = 'geopetl/tests/fixtures_data/staging/point.csv'
    return csv_dir


# return table name for oracle table based on csv file name
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


# write csv staging data to test table
@pytest.fixture
def create_test_tables(oraclesde_db, table_name, csv_dir):
    eastern = timezone('US/Eastern')
    # populate a new geopetl table object with staging data from csv file
    rows1 = etl.fromcsv(csv_dir).convert('numericfield', int)
    rows = etl.convert(rows1, ['datefield','timezone'], lambda row: dt_parser.parse(row))
    rows = etl.convert(rows, 'timezone', lambda row: row.astimezone(eastern))
    # write geopetl table to oracle
    rows.tooraclesde(oraclesde_db.dbo, table_name)



######################################   TESTS   ####################################################################

# assert number of rows
def test_all_rows_written(host, port, service_name,user, pw,csv_dir,table_name, create_test_tables): #
    # read staging data from csv
    with open(csv_dir, newline='') as f:
        reader = csv.reader(f)
        csv_data = list(reader)

    csv_row_count = len(csv_data[1:])
    dsn = cx_Oracle.makedsn(host, port, service_name=service_name)
    connection = cx_Oracle.connect(user, pw, dsn, encoding="UTF-8")

    try:
        with connection.cursor() as cursor:
            # execute the insert statement
            cursor.execute("select * from " + table_name)
            result = cursor.fetchall()
    except cx_Oracle.Error as error:
        print('Error occurred:')
        print(error)
    # get number of rows from query
    oracle_num_rows = len(result)
    assert csv_row_count == oracle_num_rows



# compare csv data with oracle data using cxoracle
def test_assert_data(csv_dir, oraclesde_db, table_name):
    eastern = timezone('US/Eastern')
    # read staging data from csv
    csv_data = etl.fromcsv(csv_dir).convert(['objectid','numericfield'], int)
    csv_data = etl.convert(csv_data,'datefield', lambda row: dt_parser.parse(row).replace(microsecond=0))
    csv_data = etl.convert(csv_data,'timezone', lambda row: dt_parser.parse(row).astimezone(eastern))

    # list of column names
    keys = csv_data[0]

    cur = oraclesde_db.dbo.cursor()
    # alter session date/timestamps formats
    cur.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'"
                " NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF'"
                " NLS_TIMESTAMP_TZ_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM'")
    # select data from table using cx-oracle
    cur.execute(
        "select objectid,textfield,datefield,numericfield, to_char(timezone,'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM'), sde.st_astext(shape) from " + table_name)
    rows = cur.fetchall()
    i = 1
    # iterate through each row of data
    for row in rows:
        # create dictionary for each row of data using same set of keys
        csv_dict = dict(zip(keys, csv_data[i]))  # dictionary from csv data
        oracle_dict = dict(zip(keys, row))  # dictionary from oracle data

        for key in keys:
            # create dictionary for each row of data using same set of keys
            if key == 'shape':
                cx_oracle_val = remove_whitespace(str(oracle_dict.get(key)))
                csv_val = remove_whitespace(str(csv_dict.get(key)))
                assert cx_oracle_val == csv_val
            # timezone data needs to be queried as a string usingto_char
            elif key == 'timezone':
                cx_oracle_val = dt_parser.parse(oracle_dict.get(key))
                csv_val = csv_dict.get(key)
                assert csv_val == cx_oracle_val
            else:
                cx_oracle_val = oracle_dict.get(key)
                csv_val = csv_dict.get(key)
                assert csv_val == cx_oracle_val
        i = i+1


# compare csv data with oracle table using geopetl
def test_assert_data_2(csv_dir, oraclesde_db, table_name):
    eastern = timezone('US/Eastern')
    # read staging data from csv
    csv_data = etl.fromcsv(csv_dir).convert(['objectid','numericfield'], int)
    csv_data = etl.convert(csv_data,'datefield', lambda row: dt_parser.parse(row).replace(microsecond=0))
    csv_data = etl.convert(csv_data,'timezone', lambda row:  dt_parser.parse(row).astimezone(eastern))
    # list of column names
    keys = csv_data[0]

    # extract test table from
    rows = etl.fromoraclesde(oraclesde_db.dbo, table_name)

    i=1
    # iterate through each row of data
    for row in rows[1:]:
        # create dictionary for each row of data using same set of keys
        csv_dict = dict(zip(keys, csv_data[i]))         # dictionary from csv data
        oracle_dict = dict(zip(keys, row))              # dictionary from oracle data
        # assert staging data with test table data
        for key in keys:
            # asserting geom field entails converting to string and removing undesired white space
            if key == 'shape':
                oracle_val = remove_whitespace(str(oracle_dict.get(key)))
                csv_geom = remove_whitespace(str(csv_dict.get(key)))
                assert csv_geom == oracle_val
            else:
                oracle_val = oracle_dict.get(key)
                csv_val = csv_dict.get(key)
                assert oracle_val == csv_val
        i=i+1
