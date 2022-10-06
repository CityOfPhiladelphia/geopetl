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
from tests_config import geom_parser, line_csv_dir, line_table_name, polygon_csv_dir, line_table_name, polygon_table_name, point_table_name, point_csv_dir, fields, multipolygon_csv_dir, multipolygon_table_name


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
def create_test_tables(srid, oraclesde_db,schema):
    connection = oraclesde_db
    cursor = connection.cursor()
    cursor.execute('''truncate table {schema}.{point_table_name}_{srid}'''.format(schema=schema,
                                                                                  point_table_name=point_table_name,
                                                                                  srid=srid))
    cursor.execute( '''
    ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'
                                NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF'
                                NLS_TIMESTAMP_TZ_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM'
     ''')
    insert_stmt = '''INSERT INTO {schema}.{point_table_name}_{srid} 
    ({text_field_name}, {timestamp_field_name}, {numeric_field_name}, {timezone_field_name}, {shape_field_name}, {date_field_name}, {objectid_field_name}) 
    VALUES 
    (:{text_field_name}, :{timestamp_field_name}, :{numeric_field_name}, TO_TIMESTAMP_TZ(:{timezone_field_name}, 'YYYY-MM-DD"T"HH24:MI:SS.FFTZH:TZM'),
     SDE.ST_GEOMETRY(:{shape_field_name}, {srid}), :{date_field_name}, SDE.GDB_UTIL.NEXT_ROWID('{schema}', '{point_table_name}_{srid}'))'''.format(
        point_table_name=point_table_name,schema=schema,srid=srid,
        text_field_name=fields.get('text_field_name'),
        timestamp_field_name=fields.get('timestamp_field_name'),
        numeric_field_name=fields.get('numeric_field_name'),
        timezone_field_name=fields.get('timezone_field_name'),
        shape_field_name=fields.get('shape_field_name'),
        date_field_name=fields.get('date_field_name'),
        objectid_field_name=fields.get('object_id_field_name')
    )
    cursor.prepare(insert_stmt)
    val_rows = [{fields.get('text_field_name'): None, fields.get('timestamp_field_name'): None,
                 fields.get('numeric_field_name'): None,fields.get('timezone_field_name'): None,
                 fields.get('shape_field_name'): 'POINT EMPTY', fields.get('date_field_name'): None},

                {fields.get('text_field_name'): 'ab#$%c', fields.get('timestamp_field_name'): '2019-05-15 15:53:53.522000',
                 fields.get('numeric_field_name'): '12', fields.get('timezone_field_name'): '2011-11-22T10:23:54-04:00',
                 fields.get('shape_field_name'): 'POINT(2712205.71100539 259685.27615705)', fields.get('date_field_name'): '2005-01-01 00:00:00'},
                
                {fields.get('text_field_name'): 'd!@^&*?-=+ef', fields.get('timestamp_field_name'): '2019-05-14 15:53:53.522000', 
                 fields.get('numeric_field_name'): '1', fields.get('timezone_field_name'): None,
                 fields.get('shape_field_name'): 'POINT(2672818.51681407 231921.15681663)',fields.get('date_field_name'): '2015-03-01 00:00:00'},
                
                {fields.get('text_field_name'): 'fij()dcfwef', fields.get('timestamp_field_name'): '2019-05-14 15:53:53.522000', 
                 fields.get('numeric_field_name'): '2132134342',fields.get('timezone_field_name'): '2014-04-11T10:23:54+05:00',
                 fields.get('shape_field_name'): 'POINT(2704440.74884506 251030.69241638)',fields.get('date_field_name'): None},
                
                {fields.get('text_field_name'): 'po{}tato', fields.get('timestamp_field_name'): '2019-05-14 15:53:53.522000', 
                 fields.get('numeric_field_name'): '0',fields.get('timezone_field_name'): '2021-08-23T10:23:54-02:00',
                 fields.get('shape_field_name'): 'POINT(2674410.98607007 233770.15508713)',fields.get('date_field_name'): '2008-08-11 00:00:00'},
                
                {fields.get('text_field_name'): 'v[]im', fields.get('timestamp_field_name'): '2019-05-14 15:53:53.522000', 
                 fields.get('numeric_field_name'): '1353',fields.get('timezone_field_name'): '2015-03-21T10:23:54-01:00', 
                 fields.get('shape_field_name'): 'POINT(2694352.72374555 250468.93894671)', fields.get('date_field_name'): '2005-09-07 00:00:00'},
                
                {fields.get('text_field_name'): 'he_llo', fields.get('timestamp_field_name'): '2019-05-14 15:53:53.522000', 
                 fields.get('numeric_field_name'): '49053',fields.get('timezone_field_name'): '2020-06-12T10:23:54+03:00',
                 fields.get('shape_field_name'): 'POINT(2675096.08410931 231074.64210546)',fields.get('date_field_name'): '2003-11-23 00:00:00'},
                
                {fields.get('text_field_name'): 'y"ea::h', fields.get('timestamp_field_name'): '2018-03-30 15:10:06', 
                 fields.get('numeric_field_name'): '-123',fields.get('timezone_field_name'): '2032-04-30T10:23:54-03:00', 
                 fields.get('shape_field_name'): 'POINT(2694560.19708389 244942.81679688)',fields.get('date_field_name'): '2020-04-01 00:00:00'},
                
                {fields.get('text_field_name'): "qwe'qeqdqw", fields.get('timestamp_field_name'): '2019-01-05 10:53:52', 
                 fields.get('numeric_field_name'): '456',fields.get('timezone_field_name'): '2018-12-25T10:23:54+00:00', 
                 fields.get('shape_field_name'): 'POINT(2680866.32552156 241245.62340388)',fields.get('date_field_name'): '2018-07-19 00:00:00'},
                
                {fields.get('text_field_name'): 'lmwefwe', fields.get('timestamp_field_name'): '2019-05-14 15:53:53.522000', 
                 fields.get('numeric_field_name'): '5654',fields.get('timezone_field_name'): '2018-12-25T10:23:54+00:00',
                 fields.get('shape_field_name'): 'POINT(2692140.13630722 231409.33008438)',fields.get('date_field_name'): '2017-06-26 00:00:00'},
                
                {fields.get('text_field_name'): None, fields.get('timestamp_field_name'): '2019-05-14 15:53:53.522000', 
                 fields.get('numeric_field_name'): '5654',fields.get('timezone_field_name'): '2018-12-25T10:23:54+00:00',
                 fields.get('shape_field_name'): 'POINT EMPTY', fields.get('date_field_name'): '2017-06-26 00:00:00'}]
    print('val_rows')
    print(val_rows)
    cursor.executemany(None, val_rows, batcherrors=False)

    connection.commit()


@pytest.fixture
def create_line_table(srid, oraclesde_db,schema):
    insert_stmt = '''INSERT INTO {schema}.{line_table_name}_{srid} ({shape_field_name}, {objectid_field_name})
     VALUES 
     (SDE.ST_GEOMETRY(:{shape_field_name}, {srid}), SDE.GDB_UTIL.NEXT_ROWID('{schema}', '{line_table_name}_{srid}'))'''.format(
        schema=schema,line_table_name=line_table_name,srid=srid,
        shape_field_name=fields.get('shape_field_name'),
        objectid_field_name=fields.get('object_id_field_name'))
    cursor = oraclesde_db.cursor()
    val_rows = [
        {fields.get('shape_field_name'): 'LINESTRING EMPTY'},
        {fields.get('shape_field_name'): 'LINESTRING(2679640.41975001 259205.68799999, 2679610.90800001 259142.53425001)'},
        {fields.get('shape_field_name'): 'LINESTRING(2679640.41975001 259205.68799999, 2679610.90800001 259142.53425001)'},
        {fields.get('shape_field_name'): 'LINESTRING(2679640.41975001 259205.68799999, 2679610.90800001 259142.53425001)'},
        {fields.get('shape_field_name'): 'LINESTRING(2679640.41975001 259205.68799999, 2679610.90800001 259142.53425001)'},
        {fields.get('shape_field_name'): 'LINESTRING(2679640.41975001 259205.68799999, 2679610.90800001 259142.53425001)'}
    ]
    cursor.execute('''truncate table {schema}.{line_table_name}_{srid}'''.format(schema=schema,line_table_name=line_table_name,srid=srid))
    cursor.prepare(insert_stmt)
    cursor.executemany(None, val_rows, batcherrors=False)
    oraclesde_db.commit()

# Loads polygon staging data using insert qry
@pytest.fixture
def create_polygon_table(srid, oraclesde_db,schema):
    insert_stmt = '''INSERT INTO {schema}.{polygon_table_name}_{srid} 
        ({shape_field_name}, {objectid_field_name}) 
        VALUES 
        (SDE.ST_GEOMETRY(:{shape_field_name}, {srid}), SDE.GDB_UTIL.NEXT_ROWID('{schema}', '{polygon_table_name}_{srid}'))'''.format(
            schema=schema, polygon_table_name=polygon_table_name,srid=srid,
            shape_field_name=fields.get('shape_field_name'),objectid_field_name=fields.get('object_id_field_name'))
    cursor = oraclesde_db.cursor()
    val_rows = [
        {fields.get('shape_field_name'): 'POLYGON EMPTY'},
        {fields.get('shape_field_name'): 'POLYGON(( 2697048.19400001 243967.35275000,2697046.11900000 244007.28925000,2697046.19599999 244038.87700000,2696984.16900000 244045.93900000,2697059.92400000 243874.43500000,2697048.19400001 243967.35275000))'},
        {fields.get('shape_field_name'): 'POLYGON(( 2697048.19400001 243967.35275000,2697046.11900000 244007.28925000,2697046.19599999 244038.87700000,2696984.16900000 244045.93900000,2697059.92400000 243874.43500000,2697048.19400001 243967.35275000))'},
        {fields.get('shape_field_name'): 'POLYGON(( 2697048.19400001 243967.35275000,2697046.11900000 244007.28925000,2697046.19599999 244038.87700000,2696984.16900000 244045.93900000,2697059.92400000 243874.43500000,2697048.19400001 243967.35275000))'},
        {fields.get('shape_field_name'): 'POLYGON(( 2697048.19400001 243967.35275000,2697046.11900000 244007.28925000,2697046.19599999 244038.87700000,2696984.16900000 244045.93900000,2697059.92400000 243874.43500000,2697048.19400001 243967.35275000))'},
        {fields.get('shape_field_name'): 'POLYGON(( 2697048.19400001 243967.35275000,2697046.11900000 244007.28925000,2697046.19599999 244038.87700000,2696984.16900000 244045.93900000,2697059.92400000 243874.43500000,2697048.19400001 243967.35275000))'}]
    cursor.execute('''truncate table {schema}.{polygon_table_name}_{srid}'''.format(schema=schema,srid=srid, polygon_table_name= polygon_table_name))
    cursor.prepare(insert_stmt)
    cursor.executemany(None, val_rows, batcherrors=False)
    oraclesde_db.commit()


# Loads multipolygon staging data using insert qry
@pytest.fixture
def create_multipolygon_table(srid, oraclesde_db,schema):
    insert_stmt = '''INSERT INTO {schema}.{multipolygon_table_name}_{srid} 
        ({shape_field_name}, {objectid_field_name}) 
        VALUES 
        (SDE.ST_GEOMETRY(:{shape_field_name}, {srid}), SDE.GDB_UTIL.NEXT_ROWID('{schema}', '{multipolygon_table_name}_{srid}'))'''.format(
            schema=schema, multipolygon_table_name=multipolygon_table_name,srid=srid,
            shape_field_name=fields.get('shape_field_name'),objectid_field_name=fields.get('object_id_field_name'))
    cursor = oraclesde_db.cursor()
    val_rows = [
        {fields.get('shape_field_name'): '''MULTIPOLYGON(((
        2697059.92403972 243874.43507531, 2697057.92404372 243872.43507931, 2697058.92404172 243871.43508130, 2697059.92403972 243872.43507931, 2697059.92403972 243874.43507531)),
        (( 2697048.19407630 243967.35286848, 2697050.19407231 243968.35286647, 2697049.19407430 243968.35286647, 2697048.19407630 243967.35286848)
           ))'''},
        {fields.get('shape_field_name'): '''MULTIPOLYGON((
           ( 2697059.82397431 243874.43507531, 2697057.92404372 243872.43507931, 2697058.92404172 243871.43508130, 2697059.92403972 243872.43507931, 2697059.82397431 243874.43507531)),
           (( 2697048.09401089 243967.35286848, 2697050.19407231 243968.35286647, 2697049.19407430 243968.35286647, 2697048.09401089 243967.35286848)
           ))'''},
        {fields.get('shape_field_name'): '''MULTIPOLYGON((
           ( 2697059.98407897 243874.43507531, 2697057.92404372 243872.43507931, 2697058.92404172 243871.43508130, 2697059.92403972 243872.43507931, 2697059.98407897 243874.43507531)),
           (( 2697048.29414172 243967.35286848, 2697050.19407231 243968.35286647, 2697049.19407430 243968.35286647, 2697048.29414172 243967.35286848)
           ))'''},
        {fields.get('shape_field_name'): '''MULTIPOLYGON((
           ( 2697059.92403972 243874.53514072, 2697057.92404372 243872.43507931, 2697058.92404172 243871.43508130, 2697059.92403972 243872.43507931, 2697059.92403972 243874.53514072)),
           (( 2697048.19407630 243967.45260581, 2697050.19407231 243968.35286647, 2697049.19407430 243968.35286647, 2697048.19407630 243967.45260581)
           ))'''}
    ]

    cursor.execute('''truncate table {schema}.{multipolygon_table_name}_{srid}'''.format(schema=schema,srid=srid, multipolygon_table_name= multipolygon_table_name))
    cursor.prepare(insert_stmt)
    cursor.executemany(None, val_rows, batcherrors=False)
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


#method to compare every value in local data and DB data
def assert_data_method(csv_data1, db_data1, srid1, schema=None, table=None, field=None):
    csv_header = csv_data1[0]
    try:
        # writing test - read with cxOracle
        db_header = [column[0] for column in db_data1.description]
        i=0
        try:
            db_data1 = db_data1.fetchall()
        except Exception as e:
            raise(e)
    except:
        # reading test - read w geopetl
        db_header = db_data1[0]
        i=1

    for row in csv_data1[1:]:
        csv_dict = dict(zip(csv_header, row))  # dictionary from csv data
        oracle_dict = dict(zip(db_header, db_data1[i]))  # dictionary from Oracle data
        for key in csv_header:
            db_val = oracle_dict.get(key)
            csv_val = csv_dict.get(key)
            if csv_val == '' and key != fields.get('shape_field_name'):
                assert db_val is None
                continue
            if key ==  fields.get('object_id_field_name'):
                continue
            if key == fields.get('numericfield'):
                if csv_val == '':
                    assert db_val is None
                else:
                    assert csv_val == db_val
            elif key == fields.get('shape_field_name'):
                if csv_val is None or csv_val == 'POINT EMPTY' or csv_val == '':
                    assert (db_val is None  or (str(db_val) == 'POINT EMPTY' or str(db_val) == 'POLYGON EMPTY') or str(db_val) == 'LINESTRING EMPTY')
                else:
                    csv_geom, csv_coords = geom_parser(csv_val, srid1)
                    db_geom, db_coords = geom_parser(db_val, srid1)
                    assert (csv_geom == db_geom and db_geom == csv_geom)
            elif key == fields.get('timezone_field_name'):
                try:
                    db_val = dt_parser.parse(db_val)
                except:
                    db_val = db_val
                try:
                    csv_val = dt_parser.parse(csv_val)
                except:
                    csv_val = csv_val
                assert db_val == csv_val
            else:
                assert csv_val == db_val
        i = i+1



######################################   READING TESTS   ####################################################################
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
        raise(error)
    # get number of rows from query
    oracle_num_rows = len(result)
    assert csv_row_count == oracle_num_rows


# # compare csv data with oracle data using geopetl
def test_reading_point_table(oraclesde_db, db_data,csv_data, srid):
    db_data1=db_data
    csv_data1=csv_data
    assert_data_method(csv_data1,db_data1, srid)

def test_reading_timestamp(db_data,csv_data, srid):
    key = fields.get('timestamp_field_name')
    assert_data_method(csv_data, db_data, srid,field=key)

def test_reading_numericfield(csv_data, db_data,srid):
    key = fields.get('numeric_field_name')
    assert_data_method(csv_data, db_data, srid,field=key)

def test_reading_datefield(csv_data, db_data,srid):
    key = fields.get('date_field_name')
    assert_data_method(csv_data, db_data, srid,field=key)

def test_reading_shape(db_data,csv_data, srid):
    key = fields.get('shape_field_name')
    assert_data_method(csv_data, db_data, srid,field=key)

def test_reading_textfield(db_data,csv_data,srid):
    key = fields.get('text_field_name')
    assert_data_method(csv_data, db_data, srid,field = key)

def test_reading_timezone(csv_data, db_data,srid):
    key = fields.get('text_field_name')
    assert_data_method(csv_data, db_data, srid,field = key)

# # assert point table data by loading and extracting data without providing schema
def test_reading_without_schema(oraclesde_db, csv_data,srid,):
    etl.tooraclesde(csv_data, oraclesde_db, '{}_{}'.format(point_table_name,srid), srid=srid)
    db_data2 = etl.fromoraclesde(dbo=oraclesde_db, table_name='{}_{}'.format(point_table_name,srid))
    assert_data_method(csv_data, db_data2, srid)

# #compare csv data with postgres data using geopetl
def test_reading_line_table(oraclesde_db, schema,srid, create_line_table):
    csv_data = etl.fromcsv(line_csv_dir).convert(['objectid'], int)
    # read data from test DB using petl
    db_data = etl.fromoraclesde(dbo=oraclesde_db, table_name='{}.{}_{}'.format(schema,line_table_name, srid))
    assert_data_method(csv_data, db_data, srid)

def test_reading_mutipolygon_table(oraclesde_db, schema,srid, create_multipolygon_table):
    csv_data = etl.fromcsv(multipolygon_csv_dir).convert([fields.get('object_id_field_name')], int)
    table_name = '{}.{}_{}'.format(schema, multipolygon_table_name, srid)
    # read data from test DB using petl
    db_data = etl.fromoraclesde(dbo=oraclesde_db, table_name='{}.{}_{}'.format(schema, multipolygon_table_name,srid))
    assert_data_method(csv_data, db_data, srid)


def test_reading_polygon_table(oraclesde_db, schema,srid, create_polygon_table):
    csv_data = etl.fromcsv(polygon_csv_dir).convert([fields.get('object_id_field_name')], int)
    # read data from test DB using petl
    db_data = etl.fromoraclesde(dbo=oraclesde_db, table_name='{}.{}_{}'.format(schema, polygon_table_name,srid))
    assert_data_method(csv_data, db_data, srid)


############################ WRITING TESTS #######################################

# load staging data with geopetl, extract with cxOracle, assert with csv data
def test_assert_written_data(oraclesde_db, csv_data, schema,srid):
    csv_data.tooraclesde(oraclesde_db, '{}.{}_{}'.format(schema, point_table_name, srid), srid=srid)
    # read data using oracle_cx
    cursor = oraclesde_db.cursor()
    cursor.execute(
        '''select {objectid_field_name} as "{objectid_field_name}",{text_field_name} as "{text_field_name}",
        {numeric_field_name} as "{numeric_field_name}",{timestamp_field_name} as "{timestamp_field_name}",
        {date_field_name} as "{date_field_name}",to_char({timezone_field_name}, 'YYYY-MM-DD HH24:MI:SS.FFTZH:TZM') as "{timezone_field_name}",
         sde.st_astext({shape_field_name}) as "{shape_field_name}" from {}.{}_{}'''.format(
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
    assert_data_method(csv_data, cursor, srid,schema=schema, table=point_table_name)

# extract data using sql arg in fromoraclesde()
def test_stmt_arg(oraclesde_db,csv_data,schema,srid):
    qry = '''select {objectid_field_name} ,{text_field_name} ,
            {numeric_field_name},{timestamp_field_name}, {date_field_name} ,
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
    )
    db_data = etl.fromoraclesde(dbo=oraclesde_db,table_name='{}.{}_{}'.format(schema, point_table_name, srid),sql=qry)
    assert_data_method(csv_data, db_data, srid)


# load csv data to oracle db without an objectid field using geopetl and assert data
def test_wrting_data_no_id(create_test_table_noid,csv_data,schema, db_data,oraclesde_db, srid):
    csv_data1 = csv_data
    cursor = oraclesde_db.cursor()
    cursor.execute(
        '''select {objectid_field_name} as "{objectid_field_name}",{text_field_name} as "{text_field_name}",
        {numeric_field_name} as "{numeric_field_name}",{timestamp_field_name} as "{timestamp_field_name}",
        {date_field_name} as "{date_field_name}",to_char({timezone_field_name}, 'YYYY-MM-DD HH24:MI:SS.FFTZH:TZM') as "{timezone_field_name}",
         sde.st_astext({shape_field_name}) as "{shape_field_name}" from {}.{}_{}'''.format(
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
    assert_data_method(csv_data1, cursor, srid,schema=schema, table=point_table_name)


def test_polygon_assertion_write(oraclesde_db, schema,srid, create_polygon_table):
    csv_data = etl.fromcsv(polygon_csv_dir).convert([fields.get('object_id_field_name')], int)
    csv_data.tooraclesde(oraclesde_db, '{}.{}_{}'.format(schema, polygon_table_name, srid), srid=srid)
    # read data from test DB using petl
    select_qry = '''select {objectid_field_name} as "{objectid_field_name}", 
    SDE.ST_AsText({shape_field_name}) as "{shape_field_name}" from {}.{}_{}'''.format(
        schema,
        polygon_table_name,
        srid,
        objectid_field_name = fields.get('object_id_field_name'),
        shape_field_name = fields.get('shape_field_name'))
    cursor = oraclesde_db.cursor()
    cursor.execute(select_qry)
    assert_data_method(csv_data, cursor, srid,schema=schema, table=point_table_name)


def test_multipolygon_assertion_write(oraclesde_db, schema,srid):
    csv_data = etl.fromcsv(multipolygon_csv_dir).convert([fields.get('object_id_field_name')], int)
    csv_data.tooraclesde(oraclesde_db, '{}.{}_{}'.format(schema, multipolygon_table_name, srid), srid=srid)
    # read data from test DB using petl
    select_qry = '''select {objectid_field_name} as "{objectid_field_name}",
    SDE.ST_AsText({shape_field_name}) as "{shape_field_name}" from {schema}.{table}_{srid}'''.format(
        schema=schema,
        table=multipolygon_table_name,
        srid=srid,
        objectid_field_name = fields.get('object_id_field_name'),
        shape_field_name = fields.get('shape_field_name'))
    cursor = oraclesde_db.cursor()
    cursor.execute(select_qry)
    assert_data_method(csv_data, cursor, srid)


def test_line_assertion_write(oraclesde_db, schema,srid):
    csv_data = etl.fromcsv(line_csv_dir).convert([fields.get('object_id_field_name')], int)
    csv_data.tooraclesde(oraclesde_db, '{}.{}_{}'.format(schema, line_table_name, srid), srid=srid)
    # read data from test DB using petl
    stmt = '''select {objectid_field_name} as "{objectid_field_name}", SDE.ST_AsText({shape_field_name}) as "{shape_field_name}" from {}.{}_{}'''.format(
        schema,
        line_table_name,
        srid,
        objectid_field_name = fields.get('object_id_field_name'),
        shape_field_name = fields.get('shape_field_name')
    )
    cursor = oraclesde_db.cursor()
    cursor.execute(stmt)
    assert_data_method(csv_data, cursor, srid,schema=schema, table=point_table_name)

# # assert DB data with itself
def test_reading_with_types(db_data,oraclesde_db, schema, srid):
    data1 = db_data
    # load to second test table
    db_data.tooraclesde(oraclesde_db, '{}.{}_{}_2'.format(schema,point_table_name,srid), srid=srid)
    # extract from second test table
    data2 = etl.fromoraclesde(dbo=oraclesde_db, table_name='{}.{}_{}_2'.format(schema,point_table_name,srid))
    assert_data_method(data1, data2, srid)
