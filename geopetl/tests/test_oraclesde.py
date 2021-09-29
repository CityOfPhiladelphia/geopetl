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

def remove_whitespace(stringval):
    print('stringval ', stringval)
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

# return Oracle database object
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
    csv_dir = 'geopetl/tests/fixtures_data/staging/point.csv'
    return csv_dir


# return table name for Oracle table based on json file name
@pytest.fixture
def table_name(csv_dir, schema):
    head_tail = os.path.split(csv_dir)
    # define which table based on schema file name
    table = ''
    if 'point' in head_tail[1]:
        table = 'point'
    elif 'polygon' in head_tail[1]:
        table = 'polygon'
    # define table name
    table_name = schema + '.' + table + '_table'
    return table_name


# write csv staging data to test table
@pytest.fixture
def create_test_tables(oraclesde_db, table_name, csv_dir):
    # populate a new geopetl table object with staging data from csv file
    rows = etl.fromcsv(csv_dir)
    # write geopetl table to oracle
    rows.tooraclesde(oraclesde_db.dbo, table_name)


######################################   TESTS   ####################################################################

# read number of rows
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
            cursor.execute("select * from "+ table_name)
            result = cursor.fetchall()
    except cx_Oracle.Error as error:
        print('Error occurred')
        print(error)

    # get number of rows from query
    oracle_num_rows = len(result)
    assert csv_row_count == oracle_num_rows



# compare csv data with oracle data using cxoracle
def test_assert_data(csv_dir, oraclesde_db, table_name):
    csv_data = etl.fromcsv(csv_dir).convert(['objectid','numericfield'], int)
    csv_data = etl.convert(csv_data,['timestamp','datefield','timezone'], lambda row: dt_parser.parse(row))

    # list of column names
    csv_header = csv_data[0]

    # read data using oracle_cx
    cur = oraclesde_db.dbo.cursor()
    cur.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD'"
                " NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF'"
                " NLS_TIMESTAMP_TZ_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FFTZH:TZM'")
    cur.execute(
        '''select objectid,textfield,numericfield,timestamp,datefield,
         to_char(timezone, 'YYYY-MM-DD HH24:MI:SS.FFTZH:TZM') as timezone,
         sde.st_astext(shape) as shape from ''' + table_name)

    db_header = [column[0] for column in cur.description]
    rows = cur.fetchall()

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
def test_assert_data_2(csv_dir, oraclesde_db, table_name):
    csv_data = etl.fromcsv(csv_dir).convert(['objectid','numericfield'], int)
    csv_data = etl.convert(csv_data,['timestamp','datefield','timezone'], lambda row: dt_parser.parse(row))

    # list of column names
    keys = csv_data[0]

    # get oracle data
    rows = etl.fromoraclesde(dbo=oraclesde_db.dbo,table_name=table_name)
    db_header = rows[0]

    i=1
    # iterate through each row of data
    for row in rows[1:]:
        # create dictionary for each row of data using same set of keys
        csv_dict = dict(zip(keys, csv_data[i]))         # dictionary from csv data
        oracle_dict = dict(zip(db_header, row))         # dictionary from oracle data
        for key in keys:
            if key == 'objectid':
                 continue
            elif key == 'shape':
                db_val = remove_whitespace(str(oracle_dict.get(key)))
                csv_val = remove_whitespace(str(csv_dict.get(key)))
                assert csv_val == db_val
            else:
                db_val = oracle_dict.get(key)
                csv_val = csv_dict.get(key)
                assert db_val == csv_val
        i=i+1
