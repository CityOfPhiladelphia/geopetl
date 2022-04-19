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
from tests_config import geom_parser, line_csv_dir, line_table_name, polygon_csv_dir, line_table_name,polygon_table_name,point_table_name, point_csv_dir, fields


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
    csv_data = etl.fromcsv(point_csv_dir).convert([fields.get('object_id_field_name'),fields.get('numeric_field_name')], int)
    csv_data = etl.convert(csv_data,[fields.get('timestamp_field_name'),fields.get('date_field_name'),fields.get('timezone_field_name')], lambda row: dt_parser.parse(row))
    return csv_data

# write staging data to test table using oracle query
@pytest.fixture
def create_test_tables(srid, oraclesde_db):
    connection = oraclesde_db

    populate_table_stmt = '''INSERT all
        INTO GIS_TEST.POINT_TABLE_{sr} ({text_field_name}, {timestamp_field_name}, {numericfield_field_name}, {timezone_field_name}, {shape_field_name}, {datefield_field_name}, {objectid_field_name})
        VALUES
        ( 'eeeefwe', '2019-05-15 15:53:53.522000', '5654', TO_TIMESTAMP_TZ('2008-12-25T10:23:54+00:00', 'YYYY-MM-DD"T"HH24:MI:SS.FFTZH:TZM'),SDE.ST_GEOMETRY('POINT EMPTY',{sr}), '2017-06-26 00:00:00',SDE.GDB_UTIL.NEXT_ROWID('GIS_TEST', 'point_table_{sr}'))
        INTO GIS_TEST.POINT_TABLE_{sr} ({text_field_name}, {timestamp_field_name}, {numericfield_field_name}, {timezone_field_name}, {shape_field_name}, {datefield_field_name}, {objectid_field_name})
        VALUES
        ('ab#$%c', '2019-05-14 15:53:53.522000', '12', TO_TIMESTAMP_TZ('2011-11-22T10:23:54-04', 'YYYY-MM-DD"T"HH24:MI:SS.FFTZH:TZM'), SDE.ST_GEOMETRY('POINT(2712205.71100539 259685.27615705)', {sr}), '2005-01-01', SDE.GDB_UTIL.NEXT_ROWID('GIS_TEST', 'point_table_{sr}'))
        INTO GIS_TEST.POINT_TABLE_{sr} ({text_field_name}, {timestamp_field_name}, {numericfield_field_name}, {timezone_field_name}, {shape_field_name}, {datefield_field_name}, {objectid_field_name})
        VALUES
        ('d!@^&*?-=+ef', '2019-05-14 15:53:53.522000', '1', TO_TIMESTAMP_TZ('2009-10-02T10:23:54+03:00', 'YYYY-MM-DD"T"HH24:MI:SS.FFTZH:TZM'), SDE.ST_GEOMETRY('POINT(2672818.51681407 231921.15681663)', {sr}), '2015-03-01 00:00:00',SDE.GDB_UTIL.NEXT_ROWID('GIS_TEST', 'point_table_{sr}'))
        INTO GIS_TEST.POINT_TABLE_{sr} ({text_field_name}, {timestamp_field_name}, {numericfield_field_name}, {timezone_field_name}, {shape_field_name}, {datefield_field_name}, {objectid_field_name})
        VALUES
        ('fij()dcfwef', '2019-05-14 15:53:53.522000', '2132134342', TO_TIMESTAMP_TZ('2014-04-11T10:23:54+05:00', 'YYYY-MM-DD"T"HH24:MI:SS.FFTZH:TZM'), SDE.ST_GEOMETRY('POINT(2704440.74884506 251030.69241638)', {sr}) , '2009-11-21 00:00:00',SDE.GDB_UTIL.NEXT_ROWID('GIS_TEST', 'point_table_{sr}'))
        INTO GIS_TEST.POINT_TABLE_{sr} ({text_field_name}, {timestamp_field_name}, {numericfield_field_name}, {timezone_field_name}, {shape_field_name}, {datefield_field_name}, {objectid_field_name})
        VALUES
        ('po{}tato','2019-05-14 15:53:53.522000','11', TO_TIMESTAMP_TZ('2021-08-23T10:23:54-02:00', 'YYYY-MM-DD"T"HH24:MI:SS.FFTZH:TZM'),SDE.ST_GEOMETRY('POINT(2674410.98607007 233770.15508713)', {sr}), '2008-08-11 00:00:00',SDE.GDB_UTIL.NEXT_ROWID('GIS_TEST', 'point_table_{sr}'))
        INTO GIS_TEST.POINT_TABLE_{sr} ({text_field_name}, {timestamp_field_name}, {numericfield_field_name}, {timezone_field_name}, {shape_field_name}, {datefield_field_name}, {objectid_field_name})
        VALUES
        ('v[]im','2019-05-14 15:53:53.522000', '1353', TO_TIMESTAMP_TZ('2015-03-21T10:23:54-01:00', 'YYYY-MM-DD"T"HH24:MI:SS.FFTZH:TZM'), SDE.ST_GEOMETRY('POINT(2694352.72374555 250468.93894671)', {sr}), '2005-09-07 00:00:00', SDE.GDB_UTIL.NEXT_ROWID('GIS_TEST', 'point_table_{sr}'))
        INTO GIS_TEST.POINT_TABLE_{sr} ({text_field_name}, {timestamp_field_name}, {numericfield_field_name}, {timezone_field_name}, {shape_field_name}, {datefield_field_name}, {objectid_field_name})
        VALUES
        ('he_llo', '2019-05-14 15:53:53.522000', '49053', TO_TIMESTAMP_TZ('2020-06-12T10:23:54+03:00', 'YYYY-MM-DD"T"HH24:MI:SS.FFTZH:TZM'), SDE.ST_GEOMETRY('POINT(2675096.08410931 231074.64210546)', {sr}), '2003-11-23 00:00:00', SDE.GDB_UTIL.NEXT_ROWID('GIS_TEST', 'point_table_{sr}'))
        INTO GIS_TEST.POINT_TABLE_{sr} ({text_field_name}, {timestamp_field_name}, {numericfield_field_name}, {timezone_field_name}, {shape_field_name}, {datefield_field_name}, {objectid_field_name})
         VALUES
        ('y"ea::h', '2018-03-30 15:10:06','123', TO_TIMESTAMP_TZ('2032-04-30T10:23:54-03:00', 'YYYY-MM-DD"T"HH24:MI:SS.FFTZH:TZM'), SDE.ST_GEOMETRY('POINT(2694560.19708389 244942.81679688)', {sr}), '2020-04-01 00:00:00', SDE.GDB_UTIL.NEXT_ROWID('GIS_TEST', 'point_table_{sr}'))
        INTO GIS_TEST.POINT_TABLE_{sr} ({text_field_name}, {timestamp_field_name}, {numericfield_field_name}, {timezone_field_name}, {shape_field_name}, {datefield_field_name}, {objectid_field_name})
        VALUES
        ('qwe''qeqdqw', '2019-01-05 10:53:52', '456', TO_TIMESTAMP_TZ('2018-12-25T10:23:54+00:00', 'YYYY-MM-DD"T"HH24:MI:SS.FFTZH:TZM'), SDE.ST_GEOMETRY('POINT(2680866.32552156 241245.62340388)', {sr}), '2018-07-19 00:00:00',SDE.GDB_UTIL.NEXT_ROWID('GIS_TEST', 'point_table_{sr}'))
        INTO GIS_TEST.POINT_TABLE_{sr} ({text_field_name}, {timestamp_field_name}, {numericfield_field_name}, {timezone_field_name}, {shape_field_name}, {datefield_field_name}, {objectid_field_name})
        VALUES
        ( 'lmwefwe', '2019-05-14 15:53:53.522000', '5654', TO_TIMESTAMP_TZ('2018-12-25T10:23:54+00:00', 'YYYY-MM-DD"T"HH24:MI:SS.FFTZH:TZM'), SDE.ST_GEOMETRY('POINT(2692140.13630722 231409.33008438)', {sr}), '2017-06-26 00:00:00',SDE.GDB_UTIL.NEXT_ROWID('GIS_TEST', 'point_table_{sr}'))
        INTO GIS_TEST.POINT_TABLE_{sr} ({text_field_name}, {timestamp_field_name}, {numericfield_field_name}, {timezone_field_name}, {shape_field_name}, {datefield_field_name}, {objectid_field_name})
        VALUES
        ('lmwefwe', '2019-05-14 15:53:53.522000', '5654', TO_TIMESTAMP_TZ('2018-12-25T10:23:54+00:00', 'YYYY-MM-DD"T"HH24:MI:SS.FFTZH:TZM'), SDE.ST_GEOMETRY('POINT EMPTY',{sr}), '2017-06-26 00:00:00',SDE.GDB_UTIL.NEXT_ROWID('GIS_TEST', 'point_table_{sr}'))
        select * from dual'''.format('''{}''',
                                     text_field_name=fields.get('text_field_name'),
                                     timestamp_field_name=fields.get('timestamp_field_name'),
                                     numericfield_field_name=fields.get('numeric_field_name'),
                                     timezone_field_name=fields.get('timezone_field_name'),
                                     shape_field_name=fields.get('shape_field_name'),
                                     datefield_field_name=fields.get('date_field_name'),
                                     objectid_field_name=fields.get('object_id_field_name'),
                                     sr=srid)
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
    INTO GIS_TEST.LINE_TABLE_{sr} ({shape_field_name}, {objectid_field_name}) 
    VALUES 
    (SDE.ST_GEOMETRY('LINESTRING(2679640.41975001 259205.68799999, 2679610.90800001 259142.53425001)', {sr}), SDE.GDB_UTIL.NEXT_ROWID('GIS_TEST', 'line_table_{sr}'))
    INTO GIS_TEST.LINE_TABLE_{sr} ({shape_field_name}, {objectid_field_name}) 
    VALUES 
    (SDE.ST_GEOMETRY('LINESTRING(2679640.41975001 259205.68799999, 2679610.90800001 259142.53425001)', {sr}), SDE.GDB_UTIL.NEXT_ROWID('GIS_TEST', 'line_table_{sr}'))
    INTO GIS_TEST.LINE_TABLE_{sr} ({shape_field_name}, {objectid_field_name}) 
    VALUES 
    (SDE.ST_GEOMETRY('LINESTRING(2679640.41975001 259205.68799999, 2679610.90800001 259142.53425001)', {sr}), SDE.GDB_UTIL.NEXT_ROWID('GIS_TEST', 'line_table_{sr}'))
    INTO GIS_TEST.LINE_TABLE_{sr} ({shape_field_name}, {objectid_field_name}) 
    VALUES 
    (SDE.ST_GEOMETRY('LINESTRING(2679640.41975001 259205.68799999, 2679610.90800001 259142.53425001)', {sr}), SDE.GDB_UTIL.NEXT_ROWID('GIS_TEST', 'line_table_{sr}'))
    INTO GIS_TEST.LINE_TABLE_{sr} ({shape_field_name}, {objectid_field_name}) 
    VALUES 
    (SDE.ST_GEOMETRY('LINESTRING(2679640.41975001 259205.68799999, 2679610.90800001 259142.53425001)', {sr}), SDE.GDB_UTIL.NEXT_ROWID('GIS_TEST', 'line_table_{sr}'))
    INTO GIS_TEST.LINE_TABLE_{sr} ({shape_field_name}, {objectid_field_name}) 
    VALUES 
    (SDE.ST_GEOMETRY('LINESTRING(2679640.41975001 259205.68799999, 2679610.90800001 259142.53425001)', {sr}), SDE.GDB_UTIL.NEXT_ROWID('GIS_TEST', 'line_table_{sr}'))
    select * from dual'''.format(sr=srid,
                                 objectid_field_name=fields.get('object_id_field_name'),
                                 shape_field_name=fields.get('shape_field_name'))
    cursor = oraclesde_db.cursor()
    cursor.execute('''truncate table GIS_TEST.LINE_TABLE_{sr}'''.format(sr=srid))
    cursor.execute(stmt)
    oraclesde_db.commit()

# Loads polygon staging data using insert qry
@pytest.fixture
def create_polygon_table(srid, oraclesde_db):
    stmt = '''
    INSERT all
    INTO GIS_TEST.POLYGON_TABLE_{sr} ({shape_field_name}, {objectid_field_name}) 
    VALUES 
    (SDE.ST_GEOMETRY('POLYGON(( 2697048.19400001 243967.35275000,2697046.11900000 244007.28925000,2697046.19599999 244038.87700000,2696984.16900000 244045.93900000,2697059.92400000 243874.43500000,2697048.19400001 243967.35275000))', {sr}), SDE.GDB_UTIL.NEXT_ROWID('GIS_TEST', 'polygon_table_{sr}'))
    INTO GIS_TEST.POLYGON_TABLE_{sr} ({shape_field_name}, {objectid_field_name}) 
    VALUES 
    (SDE.ST_GEOMETRY('POLYGON((2697048.19400001 243967.35275000,2697046.11900000 244007.28925000,2697046.19599999 244038.87700000,2696984.16900000 244045.93900000,2697059.92400000 243874.43500000,2697048.19400001 243967.35275000))', {sr}), SDE.GDB_UTIL.NEXT_ROWID('GIS_TEST', 'polygon_table_{sr}'))
    INTO GIS_TEST.POLYGON_TABLE_{sr} ({shape_field_name}, {objectid_field_name}) 
    VALUES 
    (SDE.ST_GEOMETRY('POLYGON((2697048.19400001 243967.35275000,2697046.11900000 244007.28925000,2697046.19599999 244038.87700000,2696984.16900000 244045.93900000,2697059.92400000 243874.43500000,2697048.19400001 243967.35275000))', {sr}), SDE.GDB_UTIL.NEXT_ROWID('GIS_TEST', 'polygon_table_{sr}'))
    INTO GIS_TEST.POLYGON_TABLE_{sr} ({shape_field_name}, {objectid_field_name}) 
    VALUES 
    (SDE.ST_GEOMETRY('POLYGON((2697048.19400001 243967.35275000,2697046.11900000 244007.28925000,2697046.19599999 244038.87700000,2696984.16900000 244045.93900000,2697059.92400000 243874.43500000,2697048.19400001 243967.35275000))', {sr}), SDE.GDB_UTIL.NEXT_ROWID('GIS_TEST', 'polygon_table_{sr}'))
    INTO GIS_TEST.POLYGON_TABLE_{sr} ({shape_field_name}, {objectid_field_name}) 
    VALUES 
    (SDE.ST_GEOMETRY('POLYGON((2697048.19400001 243967.35275000,2697046.11900000 244007.28925000,2697046.19599999 244038.87700000,2696984.16900000 244045.93900000,2697059.92400000 243874.43500000,2697048.19400001 243967.35275000))', {sr}), SDE.GDB_UTIL.NEXT_ROWID('GIS_TEST', 'polygon_table_{sr}'))
    INTO GIS_TEST.POLYGON_TABLE_{sr} ({shape_field_name}, {objectid_field_name}) 
    VALUES 
    (SDE.ST_GEOMETRY('POLYGON((2697048.19400001 243967.35275000,2697046.11900000 244007.28925000,2697046.19599999 244038.87700000,2696984.16900000 244045.93900000,2697059.92400000 243874.43500000,2697048.19400001 243967.35275000))', {sr}), SDE.GDB_UTIL.NEXT_ROWID('GIS_TEST', 'polygon_table_{sr}'))
    select * from dual'''.format(sr=srid,
            objectid_field_name = fields.get('object_id_field_name'),
            shape_field_name = fields.get('shape_field_name')
    )
    cursor = oraclesde_db.cursor()
    cursor.execute('''truncate table GIS_TEST.POLYGON_TABLE_{sr}'''.format(sr=srid))
    cursor.execute(stmt)
    oraclesde_db.commit()

# extracts data from db in geopetl frame
@pytest.fixture
def db_data(oraclesde_db, schema, srid):
    db_col = etl.fromoraclesde(dbo=oraclesde_db,table_name='{}.{}_{}'.format(schema,point_table_name,srid))
    return db_col

#loads staging data using geopetl
@pytest.fixture
def create_test_table_noid(oraclesde_db, schema,srid):
    csv_data = etl.fromcsv(point_csv_dir).cutout(fields.get('object_id_field_name'))
    csv_data.tooraclesde(oraclesde_db, '{}.{}_{}'.format(schema,point_table_name,srid))


######################################   READING TESTS   ####################################################################


def assert_data_method(csv_data1, db_data1, srid1, read, schema=None, table=None, field=None):
    csv_header = csv_data1[0]  #
    if read:  # if reading
        if field:
            keys = field
        db_header = db_data1[0]
    else:  # writing
        cxcursor = db_data1
        db_header = [column[0] for column in cxcursor.description]
        db_data1 = cxcursor.fetchall()

    for i, row in enumerate(csv_data1[1:]):
        csv_dict = dict(zip(csv_header, row))  # dictionary from csv data
        if read:
            oracle_dict = dict(zip(db_header, db_data1[i+1]))     # dictionary from Oracle data
        else:
            oracle_dict = dict(zip(db_header, db_data1[i]))         # dictionary from Oracle data
        for key in csv_header:
            if read:
                db_val = oracle_dict.get(key)
            else:
                db_val = oracle_dict.get(key.upper())
            csv_val = csv_dict.get(key)
            if key ==  fields.get('object_id_field_name'):
                continue
            elif key == fields.get('shape_field_name'):
                if csv_val is None or csv_val == 'POINT EMPTY' or csv_val == '':
                    assert (db_val is None  or  str(db_val) == 'POINT EMPTY')
                else:
                    csv_geom, csv_coords = geom_parser(csv_val, srid1)
                    db_geom, db_coords = geom_parser(db_val, srid1)
                    assert (csv_geom == db_geom and db_geom == csv_geom)
            elif key == fields.get('timezone_field_name'):
                if not read:
                    db_val = dt_parser.parse(db_val)
                assert csv_val == db_val
            else:
                assert csv_val == db_val

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
def test_assert_data_2(oraclesde_db, db_data,csv_data, srid):
    db_data1=db_data
    csv_data1=csv_data
    assert_data_method(csv_data1,db_data1, srid, read=True)

def test_assert_data_3( oraclesde_db, db_data,csv_data, srid):
    db_data1=db_data
    csv_data1=csv_data
    assert_data_method(csv_data1,db_data1, srid, read=True)

def test_reading_timestamp(db_data,csv_data, srid):
    key = fields.get('timestamp_field_name')
    assert_data_method(csv_data, db_data, srid,field = key, read=True)


def test_reading_numericfield(csv_data, db_data,srid):
    key = fields.get('numeric_field_name')
    assert_data_method(csv_data, db_data, srid,field = key, read=True)

def test_reading_datefield(csv_data, db_data,srid):
    key = fields.get('date_field_name')
    assert_data_method(csv_data, db_data, srid,field = key, read=True)

def test_reading_shape(db_data,csv_data, srid):
    key = fields.get('shape_field_name')
    assert_data_method(csv_data, db_data, srid,field = key, read=True)

def test_reading_textfield(db_data,csv_data,srid):
    key = fields.get('text_field_name')
    assert_data_method(csv_data, db_data, srid,field = key, read=True)


def test_reading_timezone(csv_data, db_data,srid):
    key = fields.get('text_field_name')
    assert_data_method(csv_data, db_data, srid,field = key, read=True)

# # assert DB data with itself
def test_reading_with_types(db_data,oraclesde_db, schema, srid):
    data1 = db_data
    # load to second test table
    db_data.tooraclesde(oraclesde_db, '{}.{}_{}_2'.format(schema,point_table_name,srid), srid=srid)
    # extract from second test table
    data2 = etl.fromoraclesde(dbo=oraclesde_db, table_name='{}.{}_{}_2'.format(schema,point_table_name,srid))
    assert_data_method(data1, data2, srid, read=True)

# # assert point table data by loading and extracting data without providing schema
def test_reading_without_schema(oraclesde_db, csv_data,srid,):
    etl.tooraclesde(csv_data, oraclesde_db, '{}_{}'.format(point_table_name,srid), srid=srid)
    db_data2 = etl.fromoraclesde(dbo=oraclesde_db, table_name='{}_{}'.format(point_table_name,srid))
    assert_data_method(csv_data, db_data2, srid, read=True)



# #compare csv data with postgres data using geopetl
def test_line_assertion(oraclesde_db, schema,srid, create_line_table):
    csv_data = etl.fromcsv(line_csv_dir).convert(['objectid'], int)
    # read data from test DB using petl
    db_data = etl.fromoraclesde(dbo=oraclesde_db, table_name='{}.{}_{}'.format(schema,line_table_name, srid))
    assert_data_method(csv_data, db_data, srid, read=True)



def test_polygon_assertion(oraclesde_db, schema,srid, create_polygon_table):
    csv_data = etl.fromcsv(polygon_csv_dir).convert(['objectid'], int)
    # read data from test DB using petl
    db_data = etl.fromoraclesde(dbo=oraclesde_db, table_name='{}.{}_{}'.format(schema, polygon_table_name,srid))
    assert_data_method(csv_data, db_data, srid, read=True)



############################ WRITING TESTS #######################################

#csv_data1 = list, cxcursor= cx cursor object
# def assert_write_tests(csv_data1, cxcursor,srid, schema, table):
#     csv_header = csv_data1[0]
#     db_header = [column[0] for column in cxcursor.description]
#     cxdata1 = cxcursor.fetchall()
#     # get column definiton
#     stmt = '''select DATA_TYPE, COLUMN_NAME   FROM ALL_TAB_COLS
#                     WHERE OWNER = '{}' AND
#                     TABLE_NAME = '{}_{}' AND
#                     HIDDEN_COLUMN = 'NO' '''.format(schema, table.upper(), srid)
#     cxcursor.execute(stmt)
#     res = cxcursor.fetchall()
#     column_def = dict(res)
#     for i, row in enumerate(cxdata1[0:]):
#         csv_dict = dict(zip(csv_header, csv_data1[i+1]))    # dictionary from csv data
#         oracle_dict = dict(zip(db_header, row))             # dictionary from Oracle data
#         for key in csv_header:
#             csv_val = csv_dict.get(key)
#             db_val = oracle_dict.get(key.upper())
#             # shape_column =
#             if key == 'objectid':
#                 continue
#             # assert shape field
#             elif key == column_def.get('ST_GEOMETRY').lower():
#                 if csv_val == 'POINT EMPTY' or csv_val == '':
#                     # reading with cx oracle
#                     assert str(db_val) == 'POINT EMPTY'
#                 else:
#                     csv_val = remove_whitespace(csv_val,srid)
#                     db_val = remove_whitespace(db_val,srid)
#                     assert csv_val == db_val
#             # assert timestamps with time timezone' :
#             elif key.upper() == column_def.get('TIMESTAMP(6) WITH TIME ZONE'):
#                 db_val = dt_parser.parse(db_val)
#                 assert csv_val == db_val
#             # compare values from each key
#             else:
#                 assert csv_val == db_val


# load staging data with geopetl, extract with cxOracle, assert with csv data
def test_assert_written_data(oraclesde_db, csv_data, schema,srid):
    csv_data.tooraclesde(oraclesde_db, '{}.{}_{}'.format(schema, point_table_name, srid), srid=srid)
    # read data using oracle_cx
    cursor = oraclesde_db.cursor()
    cursor.execute(
        '''select {objectid_field_name},{text_field_name},{numeric_field_name},{timestamp_field_name},{date_field_name},
         to_char({timezone_field_name}, 'YYYY-MM-DD HH24:MI:SS.FFTZH:TZM') as {timezone_field_name},
         sde.st_astext({shape_field_name}) as shape from {}.{}_{}'''.format(
            schema,
            point_table_name,
            srid,
            objectid_field_name=fields.get('object_id_field_name'),
            text_field_name=fields.get('text_field_name'),
            numeric_field_name=fields.get('numeric_field_name'),
            timestamp_field_name=fields.get('timestamp_field_name'),
            date_field_name=fields.get('date_field_name'),
            shape_field_name=fields.get('shape_field_name'),
            timezone_field_name=fields.get('timezone_field_name')
            )
    )
    assert_data_method(csv_data, cursor, srid,schema=schema, table=point_table_name, read=False)

# load csv data to oracle db without an objectid field using geopetl and assert data
def test_assert_data_no_id(create_test_table_noid,csv_data,schema, db_data,oraclesde_db, srid):
    csv_data1 = csv_data
    cursor = oraclesde_db.cursor()
    cursor.execute('''
        select {objectid_field_name},{text_field_name},{numeric_field_name},{timestamp_field_name},{date_field_name},
        to_char({timezone_field_name}, 'YYYY-MM-DD HH24:MI:SS.FFTZH:TZM') as {timezone_field_name},
        sde.st_astext({shape_field_name}) as {shape_field_name} from {}.{}_{}'''.format(
        schema,
        point_table_name,
        srid,
        objectid_field_name=fields.get('object_id_field_name'),
        text_field_name=fields.get('text_field_name'),
        numeric_field_name=fields.get('numeric_field_name'),
        timestamp_field_name=fields.get('timestamp_field_name'),
        date_field_name=fields.get('date_field_name'),
        shape_field_name=fields.get('shape_field_name'),
        timezone_field_name=fields.get('timezone_field_name')
        ))
    assert_data_method(csv_data1, cursor, srid,schema=schema, table=point_table_name, read=False)

def test_polygon_assertion_write(oraclesde_db, schema,srid, create_polygon_table):
    csv_data = etl.fromcsv(polygon_csv_dir).convert(['objectid'], int)
    csv_data.tooraclesde(oraclesde_db, '{}.{}_{}'.format(schema, polygon_table_name, srid), srid=srid)
    # read data from test DB using petl
    stmt = '''select {objectid_field_name}, SDE.ST_AsText({shape_field_name}) as {shape_field_name} from {}.{}_{}'''.format(
        schema,
        polygon_table_name,
        srid,
        objectid_field_name = fields.get('object_id_field_name'),
        shape_field_name = fields.get('shape_field_name'))
    cursor = oraclesde_db.cursor()
    cursor.execute(stmt)
    assert_data_method(csv_data, cursor, srid,schema=schema, table=point_table_name, read=False)

def test_line_assertion_write(oraclesde_db, schema,srid):
    csv_data = etl.fromcsv(line_csv_dir).convert(['objectid'], int)
    csv_data.tooraclesde(oraclesde_db, '{}.{}_{}'.format(schema, line_table_name, srid), srid=srid)
    # read data from test DB using petl
    stmt = '''select {objectid_field_name}, SDE.ST_AsText({shape_field_name}) as {shape_field_name} from {}.{}_{}'''.format(
        schema,
        line_table_name,
        srid,
        objectid_field_name = fields.get('object_id_field_name'),
        shape_field_name = fields.get('shape_field_name')
    )
    cursor = oraclesde_db.cursor()
    cursor.execute(stmt)
    assert_data_method(csv_data, cursor, srid,schema=schema, table=point_table_name, read=False)