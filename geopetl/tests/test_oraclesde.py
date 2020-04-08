import pytest
import petl as etl
import cx_Oracle
from geopetl.postgis import PostgisDatabase
from geopetl.tests.db_config import oracleDBcredentials
from geopetl.oracle_sde import OracleSdeDatabase, OracleSdeTable
import csv
import os
import datetime
from dateutil import parser as dt_parser
import re


############################################# FIXTURES ################################################################

# return postgis database object
@pytest.fixture
def oraclesde_db():
    # create connection string
    dsn = cx_Oracle.makedsn(oracleDBcredentials['host'], oracleDBcredentials['port'],
                            service_name=oracleDBcredentials['serviceName'])
    # db connection object
    connection = cx_Oracle.connect(oracleDBcredentials['user'], oracleDBcredentials['password'], dsn, encoding="UTF-8")
    # create & return OracleSdeDatabase object
    dbo = OracleSdeDatabase(connection)
    return dbo


# return csv file directory containing staging data
@pytest.fixture
def csv_dir():
    csv_dir = 'C:\\projects\\geopetl\\geopetl\\tests\\fixtures_data\\staging\\point.csv'
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
    #return table_name
    return 'SECOND_TEST'


# write csv staging data to test table
@pytest.fixture
def create_test_tables(oraclesde_db, table_name, csv_dir):
    # populate a new geopetl table object with staging data from csv file
    rows1 = etl.fromcsv(csv_dir).convert('numericfield', int)
    rows = etl.convert(rows1, 'datefield', lambda row: dt_parser.parse(row))
    print('etl.look(rows)')
    print(etl.look(rows))
    # write geopetl table to postgis
    rows.tooraclesde(oraclesde_db.dbo, table_name)



######################################   TESTS   ####################################################################

# read number of rows
def test_all_rows_written(csv_dir,table_name, create_test_tables): #
    # read staging data from csv
    with open(csv_dir, newline='') as f:
        reader = csv.reader(f)
        csv_data = list(reader)

    csv_row_count = len(csv_data[1:])

    dsn = cx_Oracle.makedsn(oracleDBcredentials['host'], oracleDBcredentials['port'],
                            service_name=oracleDBcredentials['serviceName'])

    connection = cx_Oracle.connect(oracleDBcredentials['user'], oracleDBcredentials['password'], dsn, encoding="UTF-8")
    try:
        with connection.cursor() as cursor:
            # execute the insert statement
            cursor.execute("select * from "+ table_name)
            #connection.commit()??????# commit work
            result = cursor.fetchall()
    except cx_Oracle.Error as error:
        print('Error occurred:')
        print(error)

    # get number of rows from query
    oracle_num_rows = len(result)
    assert csv_row_count == oracle_num_rows



# compare csv data with oracle data using oraclecx
def test_assert_data(csv_dir, oraclesde_db, table_name):
    # read staging data from csv
    # with open(csv_dir, newline='') as f:
    #     reader = csv.reader(f)
    #     csv_data = list(reader)

    csv_data = etl.fromcsv(csv_dir)
    csv_data = etl.fromcsv(csv_dir).convert('numericfield', int)
    csv_data = etl.convert(csv_data, 'datefield', lambda row: dt_parser.parse(row))
    # list of column names
    keys = csv_data[0]

    # read data using postgis
    cur = oraclesde_db.dbo.cursor()
    cur.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'"
                " NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF'"
                " NLS_TIMESTAMP_TZ_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM'")
    cur.execute(
        'select objectid,textfield,datefield,numericfield,sde.st_astext(shape) from ' + table_name)  # sde.st_astext(shape)
    rows = cur.fetchall()

    i = 1
    # iterate through each row of data
    for row in rows:
        # create dictionary for each row of data using same set of keys
        csv_dict = dict(zip(keys, csv_data[i]))  # dictionary from csv data
        oracle_dict = dict(zip(keys, row))  # dictionary from postgis data

        for key in keys:
            if key == 'datefield':
                df1 = oracle_dict.get('datefield')
                df2 = csv_dict.get('datefield')
                df2 = df2.replace(microsecond=0)
                print(key + ' ' + str(df1 == df2))
                assert df1 == df2
            elif key == 'numericfield':
                nf = int(oracle_dict.get('numericfield'))
                nf2 = int(csv_dict.get('numericfield'))
                print(type(nf2), type(nf))
                print(key + ' ' + str(nf == nf2))
                assert nf == nf2
            elif key == 'shape':
                shapestring = str(oracle_dict.get('shape'))
                coordinates = re.findall("\d+\.\d+", shapestring)
                if "point" or "POINT" in shapestring:
                    geom = "POINT({x})".format(x=" ".join(coordinates))
                print(key + ' ' + str(geom == csv_dict.get('shape')))
                assert geom == csv_dict.get('shape')
            else:
                val1 = str(oracle_dict.get(key))
                val2 = str(csv_dict.get(key))
                print(key + ' ' + str(val1 == val2))
                assert val1 == val2
            print('\n')
        i = i + 1


# # compare csv data with postgres data using geopetl
def test_assert_data_2(csv_dir, oraclesde_db, table_name):
    # read staging data from csv
    # with open(csv_dir, newline='') as f:
    #     reader = csv.reader(f)
    #     csv_data = list(reader)

    csv_data = etl.fromcsv(csv_dir)
    csv_data = etl.fromcsv(csv_dir).convert('numericfield', int)
    csv_data = etl.convert(csv_data,'datefield', lambda row: dt_parser.parse(row))
    # list of column names
    keys = csv_data[0]

    # written_table = etl.fromdb(dbo.dbo, table_name)
    rows = etl.fromoraclesde(oraclesde_db.dbo, table_name)

    i=1
    # iterate through each row of data
    for row in rows[1:]:
        # create dictionary for each row of data using same set of keys
        csv_dict = dict(zip(keys, csv_data[i]))         # dictionary from csv data
        oracle_dict = dict(zip(keys, row))              # dictionary from postgis data

        for key in keys:
            if key =='datefield':
                df1 = oracle_dict.get('datefield')
                df2 = csv_dict.get('datefield')
                df2 = df2.replace(microsecond=0)
                print(key + ' ' + str(df1 == df2))
                assert df1 ==df2
            elif key == 'numericfield':
                nf = int(oracle_dict.get('numericfield'))
                nf2 = int(csv_dict.get('numericfield'))
                print(key + ' ' + str(nf == nf2))
                assert nf == nf2
            elif key == 'shape':
                shapestring = str(oracle_dict.get('shape'))
                coordinates = re.findall("\d+\.\d+", shapestring)
                if "point" or "POINT" in shapestring:
                    geom = "POINT({x})".format(x=" ".join(coordinates))
                print(key + ' ' + str(geom == csv_dict.get('shape')))
                assert geom == csv_dict.get('shape')
            else:
                val1 = str(oracle_dict.get(key))
                val2 = str(csv_dict.get(key))
                print(key + ' '+ str(val1==val2))
                assert val1 == val2
            print('\n')
        i=i+1
