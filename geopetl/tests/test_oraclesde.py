




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


# def test_assert_data_2( oraclesde_db, db_data,csv_data,schema, srid):
#     myDB= OracleSdeDatabase(oraclesde_db)
#     tb = myDB.table('{}.{}_{}'.format(schema, point_table_name, srid))
#     # list of column names
#     keys = csv_data[0]
#     i=1
#     # iterate through each row of data
#     for row in db_data[1:]:
#         # create dictionary for each row of data using same set of keys
#         csv_dict = dict(zip(keys, csv_data[i]))         # dictionary from csv data
#         oracle_dict = dict(zip(db_data[0], row))        # dictionary from Oracle data
#
#         for key in keys:
#             if key == tb.geom_field:
#                 csv_val = csv_dict.get(key.upper())
#                 print('dbval "{}"'.format(oracle_dict.get(key.upper())))
#                 print('csv_val ', csv_val)
#                 if csv_val is None or csv_val == 'POINT EMPTY':
#                     print('182 csv val is none ')
#                     print('db val is POINT EMPTY ', str(oracle_dict.get(key.upper())) == 'POINT EMPTY')
#                     assert oracle_dict.get(key.upper()) is None
#                 else:
#                     pg_geom = remove_whitespace(str(oracle_dict.get(tb.geom_field)))
#                     csv_geom = remove_whitespace(str(csv_dict.get(tb.geom_field)))
#                     assert csv_geom == pg_geom
#             elif key =='objectid':
#                 continue
#             else:
#                 val1 = oracle_dict.get(key)
#                 val2 = csv_dict.get(key)
#                 assert val1 == val2
#         i=i+1

############################################# FIXTURES ################################################################

# return Oracle database connection object
@pytest.fixture
def oraclesde_db(host, port, service_name,user, pw):
    dsn = cx_Oracle.makedsn(host, port, service_name=service_name)
    # db connection object
    connection = cx_Oracle.connect(user, pw, dsn, encoding="UTF-8")
    return connection

# returns csv data in geopetl data frame
@pytest.fixture
def csv_data():
    csv_data = etl.fromcsv(point_csv_dir).convert(['objectid','numericfield'], int)
    csv_data = etl.convert(csv_data,['timestamp','datefield','timezone'], lambda row: dt_parser.parse(row))
    return csv_data

# write staging data to test table using oracle query
@pytest.fixture
def create_test_tables(srid, oraclesde_db):
    connection = oraclesde_db
    populate_table_stmt = '''INSERT all
    INTO GIS_TEST.POINT_TABLE_{sr} (textfield, timestamp, numericfield, timezone, shape, datefield, objectid)
    VALUES
    ( 'eeeefwe', '2019-05-15 15:53:53.522000', '5654', TO_TIMESTAMP_TZ('2008-12-25T10:23:54+00:00', 'YYYY-MM-DD"T"HH24:MI:SS.FFTZH:TZM'),SDE.ST_GEOMETRY('POINT EMPTY',{sr}), '2017-06-26 00:00:00',SDE.GDB_UTIL.NEXT_ROWID('GIS_TEST', 'point_table_{sr}'))
    INTO GIS_TEST.POINT_TABLE_{sr} (textfield, timestamp, numericfield, timezone, shape, datefield, objectid)
    VALUES
    ('ab#$%c', '2019-05-14 15:53:53.522000', '12', TO_TIMESTAMP_TZ('2011-11-22T10:23:54-04', 'YYYY-MM-DD"T"HH24:MI:SS.FFTZH:TZM'), SDE.ST_GEOMETRY('POINT(2712205.71100539 259685.27615705)', {sr}), '2005-01-01', SDE.GDB_UTIL.NEXT_ROWID('GIS_TEST', 'point_table_{sr}'))
    INTO GIS_TEST.POINT_TABLE_{sr} (textfield, timestamp, numericfield, timezone, shape, datefield, objectid)
    VALUES
    ('d!@^&*?-=+ef', '2019-05-14 15:53:53.522000', '1', TO_TIMESTAMP_TZ('2009-10-02T10:23:54+03:00', 'YYYY-MM-DD"T"HH24:MI:SS.FFTZH:TZM'), SDE.ST_GEOMETRY('POINT(2672818.51681407 231921.15681663)', {sr}), '2015-03-01 00:00:00',SDE.GDB_UTIL.NEXT_ROWID('GIS_TEST', 'point_table_{sr}'))
    INTO GIS_TEST.POINT_TABLE_{sr} (textfield, timestamp, numericfield, timezone, shape, datefield, objectid)
    VALUES
    ('fij()dcfwef', '2019-05-14 15:53:53.522000', '2132134342', TO_TIMESTAMP_TZ('2014-04-11T10:23:54+05:00', 'YYYY-MM-DD"T"HH24:MI:SS.FFTZH:TZM'), SDE.ST_GEOMETRY('POINT(2704440.74884506 251030.69241638)', {sr}) , '2009-11-21 00:00:00',SDE.GDB_UTIL.NEXT_ROWID('GIS_TEST', 'point_table_{sr}'))
    INTO GIS_TEST.POINT_TABLE_{sr} (textfield, timestamp, numericfield, timezone, shape, datefield, objectid)
    VALUES
    ('po{}tato','2019-05-14 15:53:53.522000','11', TO_TIMESTAMP_TZ('2021-08-23T10:23:54-02:00', 'YYYY-MM-DD"T"HH24:MI:SS.FFTZH:TZM'),SDE.ST_GEOMETRY('POINT(2674410.98607007 233770.15508713)', {sr}), '2008-08-11 00:00:00',SDE.GDB_UTIL.NEXT_ROWID('GIS_TEST', 'point_table_{sr}'))
    INTO GIS_TEST.POINT_TABLE_{sr} (textfield, timestamp, numericfield, timezone, shape, datefield, objectid)
    VALUES
    ('v[]im','2019-05-14 15:53:53.522000', '1353', TO_TIMESTAMP_TZ('2015-03-21T10:23:54-01:00', 'YYYY-MM-DD"T"HH24:MI:SS.FFTZH:TZM'), SDE.ST_GEOMETRY('POINT(2694352.72374555 250468.93894671)', {sr}), '2005-09-07 00:00:00', SDE.GDB_UTIL.NEXT_ROWID('GIS_TEST', 'point_table_{sr}'))
    INTO GIS_TEST.POINT_TABLE_{sr} (textfield, timestamp, numericfield, timezone, shape, datefield, objectid)
    VALUES
    ('he_llo', '2019-05-14 15:53:53.522000', '49053', TO_TIMESTAMP_TZ('2020-06-12T10:23:54+03:00', 'YYYY-MM-DD"T"HH24:MI:SS.FFTZH:TZM'), SDE.ST_GEOMETRY('POINT(2675096.08410931 231074.64210546)', {sr}), '2003-11-23 00:00:00', SDE.GDB_UTIL.NEXT_ROWID('GIS_TEST', 'point_table_{sr}'))
    INTO GIS_TEST.POINT_TABLE_{sr} (textfield, timestamp, numericfield, timezone, shape, datefield, objectid)
     VALUES
    ('y"ea::h', '2018-03-30 15:10:06','123', TO_TIMESTAMP_TZ('2032-04-30T10:23:54-03:00', 'YYYY-MM-DD"T"HH24:MI:SS.FFTZH:TZM'), SDE.ST_GEOMETRY('POINT(2694560.19708389 244942.81679688)', {sr}), '2020-04-01 00:00:00', SDE.GDB_UTIL.NEXT_ROWID('GIS_TEST', 'point_table_{sr}'))
    INTO GIS_TEST.POINT_TABLE_{sr} (textfield, timestamp, numericfield, timezone, shape, datefield, objectid)
    VALUES
    ('qwe''qeqdqw', '2019-01-05 10:53:52', '456', TO_TIMESTAMP_TZ('2018-12-25T10:23:54+00:00', 'YYYY-MM-DD"T"HH24:MI:SS.FFTZH:TZM'), SDE.ST_GEOMETRY('POINT(2680866.32552156 241245.62340388)', {sr}), '2018-07-19 00:00:00',SDE.GDB_UTIL.NEXT_ROWID('GIS_TEST', 'point_table_{sr}'))
    INTO GIS_TEST.POINT_TABLE_{sr} (textfield, timestamp, numericfield, timezone, shape, datefield, objectid)
    VALUES
    ( 'lmwefwe', '2019-05-14 15:53:53.522000', '5654', TO_TIMESTAMP_TZ('2018-12-25T10:23:54+00:00', 'YYYY-MM-DD"T"HH24:MI:SS.FFTZH:TZM'), SDE.ST_GEOMETRY('POINT(2692140.13630722 231409.33008438)', {sr}), '2017-06-26 00:00:00',SDE.GDB_UTIL.NEXT_ROWID('GIS_TEST', 'point_table_{sr}'))
    INTO GIS_TEST.POINT_TABLE_{sr} (textfield, timestamp, numericfield, timezone, shape, datefield, objectid)
    VALUES
    ('lmwefwe', '2019-05-14 15:53:53.522000', '5654', TO_TIMESTAMP_TZ('2018-12-25T10:23:54+00:00', 'YYYY-MM-DD"T"HH24:MI:SS.FFTZH:TZM'), SDE.ST_GEOMETRY('POINT EMPTY',{sr}), '2017-06-26 00:00:00',SDE.GDB_UTIL.NEXT_ROWID('GIS_TEST', 'point_table_{sr}'))
    select * from dual'''.format('''{}''',sr=srid)
    cursor = connection.cursor()
    cursor.execute('''truncate table GIS_TEST.POINT_TABLE_{sr}'''.format(sr=srid))
    cursor.execute( '''
    ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'
                                NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF'
                                NLS_TIMESTAMP_TZ_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM'
     ''')
    cursor.execute(populate_table_stmt)
    connection.commit()


@pytest.fixture
def create_line_table(srid, oraclesde_db):
    stmt = '''	
    INSERT all
    INTO GIS_TEST.LINE_TABLE_{sr} (shape, objectid) 
    VALUES 
    (SDE.ST_GEOMETRY('LINESTRING(2679640.41975001 259205.68799999, 2679610.90800001 259142.53425001)', {sr}), SDE.GDB_UTIL.NEXT_ROWID('GIS_TEST', 'line_table_{sr}'))
    INTO GIS_TEST.LINE_TABLE_{sr} (shape, objectid) 
    VALUES 
    (SDE.ST_GEOMETRY('LINESTRING(2679640.41975001 259205.68799999, 2679610.90800001 259142.53425001)', {sr}), SDE.GDB_UTIL.NEXT_ROWID('GIS_TEST', 'line_table_{sr}'))
    INTO GIS_TEST.LINE_TABLE_{sr} (shape, objectid) 
    VALUES 
    (SDE.ST_GEOMETRY('LINESTRING(2679640.41975001 259205.68799999, 2679610.90800001 259142.53425001)', {sr}), SDE.GDB_UTIL.NEXT_ROWID('GIS_TEST', 'line_table_{sr}'))
    INTO GIS_TEST.LINE_TABLE_{sr} (shape, objectid) 
    VALUES 
    (SDE.ST_GEOMETRY('LINESTRING(2679640.41975001 259205.68799999, 2679610.90800001 259142.53425001)', {sr}), SDE.GDB_UTIL.NEXT_ROWID('GIS_TEST', 'line_table_{sr}'))
    INTO GIS_TEST.LINE_TABLE_{sr} (shape, objectid) 
    VALUES 
    (SDE.ST_GEOMETRY('LINESTRING(2679640.41975001 259205.68799999, 2679610.90800001 259142.53425001)', {sr}), SDE.GDB_UTIL.NEXT_ROWID('GIS_TEST', 'line_table_{sr}'))
    INTO GIS_TEST.LINE_TABLE_{sr} (shape, objectid) 
    VALUES 
    (SDE.ST_GEOMETRY('LINESTRING(2679640.41975001 259205.68799999, 2679610.90800001 259142.53425001)', {sr}), SDE.GDB_UTIL.NEXT_ROWID('GIS_TEST', 'line_table_{sr}'))
    select * from dual'''.format(sr=srid)
    cursor = oraclesde_db.cursor()
    cursor.execute('''truncate table GIS_TEST.LINE_TABLE_{sr}'''.format(sr=srid))
    cursor.execute(stmt)
    oraclesde_db.commit()

# Loads
@pytest.fixture
def create_polygon_table(srid, oraclesde_db):
    stmt = '''
    INSERT all
    INTO GIS_TEST.POLYGON_TABLE_{sr} (shape, objectid) 
    VALUES 
    (SDE.ST_GEOMETRY('POLYGON(( 2697048.19400001 243967.35275000,2697046.11900000 244007.28925000,2697046.19599999 244038.87700000,2696984.16900000 244045.93900000,2697059.92400000 243874.43500000,2697048.19400001 243967.35275000))', {sr}), SDE.GDB_UTIL.NEXT_ROWID('GIS_TEST', 'polygon_table_{sr}'))
    INTO GIS_TEST.POLYGON_TABLE_{sr} (shape, objectid) 
    VALUES 
    (SDE.ST_GEOMETRY('POLYGON((2697048.19400001 243967.35275000,2697046.11900000 244007.28925000,2697046.19599999 244038.87700000,2696984.16900000 244045.93900000,2697059.92400000 243874.43500000,2697048.19400001 243967.35275000))', {sr}), SDE.GDB_UTIL.NEXT_ROWID('GIS_TEST', 'polygon_table_{sr}'))
    INTO GIS_TEST.POLYGON_TABLE_{sr} (shape, objectid) 
    VALUES 
    (SDE.ST_GEOMETRY('POLYGON((2697048.19400001 243967.35275000,2697046.11900000 244007.28925000,2697046.19599999 244038.87700000,2696984.16900000 244045.93900000,2697059.92400000 243874.43500000,2697048.19400001 243967.35275000))', {sr}), SDE.GDB_UTIL.NEXT_ROWID('GIS_TEST', 'polygon_table_{sr}'))
    INTO GIS_TEST.POLYGON_TABLE_{sr} (shape, objectid) 
    VALUES 
    (SDE.ST_GEOMETRY('POLYGON((2697048.19400001 243967.35275000,2697046.11900000 244007.28925000,2697046.19599999 244038.87700000,2696984.16900000 244045.93900000,2697059.92400000 243874.43500000,2697048.19400001 243967.35275000))', {sr}), SDE.GDB_UTIL.NEXT_ROWID('GIS_TEST', 'polygon_table_{sr}'))
    INTO GIS_TEST.POLYGON_TABLE_{sr} (shape, objectid) 
    VALUES 
    (SDE.ST_GEOMETRY('POLYGON((2697048.19400001 243967.35275000,2697046.11900000 244007.28925000,2697046.19599999 244038.87700000,2696984.16900000 244045.93900000,2697059.92400000 243874.43500000,2697048.19400001 243967.35275000))', {sr}), SDE.GDB_UTIL.NEXT_ROWID('GIS_TEST', 'polygon_table_{sr}'))
    INTO GIS_TEST.POLYGON_TABLE_{sr} (shape, objectid) 
    VALUES 
    (SDE.ST_GEOMETRY('POLYGON((2697048.19400001 243967.35275000,2697046.11900000 244007.28925000,2697046.19599999 244038.87700000,2696984.16900000 244045.93900000,2697059.92400000 243874.43500000,2697048.19400001 243967.35275000))', {sr}), SDE.GDB_UTIL.NEXT_ROWID('GIS_TEST', 'polygon_table_{sr}'))
    select * from dual'''.format(sr=srid)
    cursor = oraclesde_db.cursor()
    cursor.execute('''truncate table GIS_TEST.POLYGON_TABLE_{sr}'''.format(sr=srid))
    cursor.execute(stmt)
    oraclesde_db.commit()

# extract data from database using geopetl
@pytest.fixture
# extracts data from db in geopetl frame
def db_data(oraclesde_db, schema, srid):
    db_col = etl.fromoraclesde(dbo=oraclesde_db,table_name='{}.{}_{}'.format(schema,point_table_name,srid))
    return db_col

#loads staging data using geopetl
@pytest.fixture
def create_test_table_noid(oraclesde_db, schema,srid):
    csv_data = etl.fromcsv(point_csv_dir).cutout('objectid')
    csv_data.tooraclesde(oraclesde_db, '{}.{}_{}'.format(schema,point_table_name,srid))



######################################   READING TESTS   ####################################################################
def assert_readings(db_data1, csv_data1, field=None):
    keys = csv_data1[0]
    if field:
        keys = [field]

    for i, row in enumerate(csv_data1[1:]):
        csv_dict = dict(zip(csv_data1[0], row))     # dictionary from csv data
        oracle_dict = dict(zip(db_data1[0], db_data1[i+1]))    # dictionary from Oracle data
        # iterate through each keys
        for key in keys:
            db_val = oracle_dict.get(key)
            csv_val = csv_dict.get(key)
            if key == 'objectid':
                continue
            # assert shape field
            elif key == 'shape':
                if csv_val is None or csv_val == 'POINT EMPTY' or csv_val=='':
                    assert db_val is None
                else:
                    db_geom = remove_whitespace(str(db_val))
                    csv_geom = remove_whitespace(str(csv_val))
                    assert csv_geom == db_geom
        # compare values from each key
            else:
                assert csv_val == db_val
        i = i + 1

# read number of rows
def test_all_rows_written(host, port, service_name,user, pw,schema, create_test_tables, srid): #
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
            cursor.execute("select * from {}.{}_{} ".format(schema,point_table_name,srid))
            result = cursor.fetchall()
    except cx_Oracle.Error as error:
        print('Error occurred')
        print(error)

    # get number of rows from query
    oracle_num_rows = len(result)
    assert csv_row_count == oracle_num_rows


# # compare csv data with oracle data using geopetl
def test_assert_data_2( oraclesde_db, db_data,csv_data,schema, srid):
    # list of column names
    db_data1= db_data
    csv_data1 = csv_data
    assert_readings(db_data1, csv_data1)


def test_reading_timestamp(csv_data, db_data, srid):
    key = 'timestamp'
    assert_readings(db_data, csv_data, key)


def test_reading_numericfield(csv_data, db_data):
    key = 'numericfield'
    assert_readings(db_data, csv_data, key)

def test_reading_datefield(csv_data, db_data):
    key = 'datefield'
    assert_readings(db_data, csv_data, key)

def test_reading_shape(csv_data, db_data):
    key = 'shape'
    assert_readings(db_data, csv_data, key)

def test_reading_textfield(csv_data, db_data):
    key = 'textfield'
    assert_readings(db_data, csv_data, key)

def test_reading_timezone(csv_data, db_data):
    key = 'timezone'
    assert_readings(db_data, csv_data, key)

# # assert DB data with itself
def test_reading_with_types(db_data,oraclesde_db, schema, srid):
    data1 = db_data
    # load to second test table
    db_data.tooraclesde(oraclesde_db, '{}.{}_{}_2'.format(schema,point_table_name,srid), srid=srid)
    # extract from second test table
    data2 = etl.fromoraclesde(dbo=oraclesde_db, table_name='{}.{}_{}_2'.format(schema,point_table_name,srid))
    assert_readings(data1, data2, srid)

# # assert point table data by loading and extracting data without providing schema
def test_reading_without_schema(oraclesde_db, csv_data,srid,):
    etl.tooraclesde(csv_data, oraclesde_db, point_table_name)
    db_data2 = etl.fromoraclesde(dbo=oraclesde_db, table_name=point_table_name)
    assert_readings(csv_data, db_data2, srid)

# #compare csv data with postgres data using geopetl
def test_line_assertion(oraclesde_db, schema,srid, create_line_table):
    myDB= OracleSdeDatabase(oraclesde_db)
    csv_data = etl.fromcsv(line_csv_dir).convert(['objectid'], int)
    # read data from test DB using petl
    db_data = etl.fromoraclesde(dbo=oraclesde_db, table_name='{}.{}_{}'.format(schema,line_table_name, srid))
    assert_readings(csv_data, db_data, srid)


def test_polygon_assertion(oraclesde_db, schema,srid, create_polygon_table):
    csv_data = etl.fromcsv(polygon_csv_dir).convert(['objectid'], int)
    # read data from test DB using petl
    db_data = etl.fromoraclesde(dbo=oraclesde_db, table_name='{}.{}_{}'.format(schema, polygon_table_name,srid))
    assert_readings(csv_data, db_data, srid)






############################ WRITING TESTS #######################################

#csv_data1 = list, cxcursor= cx cursor object
def assert_write_tests(csv_data1, cxcursor,srid):
    csv_header = csv_data1[0]
    db_header = [column[0] for column in cxcursor.description]
    cxdata1 = cxcursor.fetchall()
    for i, row in enumerate(cxdata1[0:]):
        csv_dict = dict(zip(csv_header, csv_data1[i+1]))    # dictionary from csv data
        oracle_dict = dict(zip(db_header, row))             # dictionary from Oracle data
        for key in csv_header:
            csv_val = csv_dict.get(key)
            db_val = oracle_dict.get(key.upper())
            if key == 'objectid':
                continue
            # assert shape field
            elif key == 'shape':
                if csv_val == 'POINT EMPTY' or csv_val == '':
                    # reading with cx oracle
                    assert str(db_val)  == 'POINT EMPTY'
                else:
                    csv_val = remove_whitespace(csv_val,srid)
                    db_val = remove_whitespace(db_val,srid)
                    assert csv_val == db_val
            elif key == 'timezone':
                db_val =  dt_parser.parse(db_val)
                assert csv_val == db_val
            # compare values from each key
            else:
                assert csv_val == db_val
        i = i + 1


# load staging data with geopetl, extract with cxOracle, assert with csv data
def test_assert_written_data(oraclesde_db, csv_data, schema,srid):
    csv_data.tooraclesde(oraclesde_db, '{}.{}_{}'.format(schema, point_table_name, srid), srid=srid)
    # read data using oracle_cx
    cursor = oraclesde_db.cursor()
    cursor.execute(
        '''select objectid,textfield,numericfield,timestamp,datefield,
         to_char(timezone, 'YYYY-MM-DD HH24:MI:SS.FFTZH:TZM') as timezone,
         sde.st_astext(shape) as shape from {}.{}_{}'''.format(schema, point_table_name, srid))
    assert_write_tests(csv_data, cursor,srid)

# load csv data to oracle db without an objectid field using geopetl and assert data
def test_assert_data_no_id(create_test_table_noid,csv_data,schema, db_data,oraclesde_db, srid):
    csv_data1 = csv_data
    db_data1 =db_data
    assert_readings(db_data1, csv_data1)

def test_polygon_assertion_write(oraclesde_db, schema,srid, create_polygon_table):
    csv_data = etl.fromcsv(polygon_csv_dir).convert(['objectid'], int)
    csv_data.tooraclesde(oraclesde_db, '{}.{}_{}'.format(schema, polygon_table_name, srid), srid=srid)
    # read data from test DB using petl
    stmt = '''select objectid, SDE.ST_AsText(shape) as shape from {}.{}_{}'''.format(schema, polygon_table_name,srid)
    cursor = oraclesde_db.cursor()
    cursor.execute(stmt)
    assert_write_tests(csv_data, cursor,srid)

def test_line_assertion_write(oraclesde_db, schema,srid):
    csv_data = etl.fromcsv(line_csv_dir).convert(['objectid'], int)
    csv_data.tooraclesde(oraclesde_db, '{}.{}_{}'.format(schema, line_table_name, srid), srid=srid)
    # read data from test DB using petl
    stmt = '''select objectid, SDE.ST_AsText(shape) as shape from {}.{}_{}'''.format(schema, line_table_name,srid)
    cursor = oraclesde_db.cursor()
    cursor.execute(stmt)
    assert_write_tests(csv_data, cursor,srid)
