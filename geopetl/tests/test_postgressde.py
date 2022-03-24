import pytest
import petl as etl
from geopetl.postgis import PostgisDatabase
import psycopg2
from pytz import timezone
import csv
from dateutil import parser as dt_parser
from tests_config import geom_parser, line_csv_dir, line_table_name, polygon_csv_dir, line_table_name,polygon_table_name, point_table_name, point_csv_dir,fields


############################################# FIXTURES ################################################################

# return postgres database object
@pytest.fixture
def postgis(db, user, pw, host):
    # create connection string
    dsn = "host={0} dbname={1} user={2} password={3}".format(host,db,user,pw)
    # create & return geopetl postgis object
    postgis_db = PostgisDatabase(dsn)
    return postgis_db

@pytest.fixture
def create_line_table(srid, postgis,schema):
    stmt = '''	
    INSERT INTO {schema}.LINE_TABLE_{sr} ({shape_field_name}, {objectid_field_name}) 
    VALUES
    (SDE.ST_GEOMETRY('LINESTRING(2679640.41975001 259205.68799999, 2679610.90800001 259142.53425001)', {sr}), SDE.GDB_UTIL.NEXT_ROWID('GIS_TEST', 'line_table_{sr}')),
    (SDE.ST_GEOMETRY('LINESTRING(2679640.41975001 259205.68799999, 2679610.90800001 259142.53425001)', {sr}), SDE.GDB_UTIL.NEXT_ROWID('GIS_TEST', 'line_table_{sr}')),
    (SDE.ST_GEOMETRY('LINESTRING(2679640.41975001 259205.68799999, 2679610.90800001 259142.53425001)', {sr}), SDE.GDB_UTIL.NEXT_ROWID('GIS_TEST', 'line_table_{sr}')),
    (SDE.ST_GEOMETRY('LINESTRING(2679640.41975001 259205.68799999, 2679610.90800001 259142.53425001)', {sr}), SDE.GDB_UTIL.NEXT_ROWID('GIS_TEST', 'line_table_{sr}')),
    (SDE.ST_GEOMETRY('LINESTRING(2679640.41975001 259205.68799999, 2679610.90800001 259142.53425001)', {sr}), SDE.GDB_UTIL.NEXT_ROWID('GIS_TEST', 'line_table_{sr}')), 
    (SDE.ST_GEOMETRY('LINESTRING(2679640.41975001 259205.68799999, 2679610.90800001 259142.53425001)', {sr}), SDE.GDB_UTIL.NEXT_ROWID('GIS_TEST', 'line_table_{sr}'))
    '''.format(schema=schema,
                sr=srid,
                objectid_field_name=fields.get('object_id_field_name'),
                shape_field_name=fields.get('shape_field_name'))
    connection = postgis.dbo
    cursor = connection.cursor()
    cursor.execute('''truncate table {schema}.LINE_TABLE_{sr}'''.format(schema= schema, sr=srid))
    cursor.execute(stmt)
    connection.commit()

# create new table and write csv staging data to it
@pytest.fixture
def create_point_table(postgis, column_definition,schema, srid):
    # write staging data to test table using oracle query
    # # populate a new geopetl table object with staging data from csv file
    # rows = etl.fromcsv(point_csv_dir)
    # # write geopetl table to postpostgis
    # rows.topostgis(postgis.dbo, '{}.{}_{}'.format(schema,point_table_name,srid), column_definition_json=column_definition, from_srid=srid)

    connection = postgis.dbo
    populate_table_stmt = ''' INSERT INTO {schema}.point_table_{sr} ({objectid_field_name}, {text_field_name}, {timestamp_field_name}, {numeric_field_name}, {timezone_field_name}, {shape_field_name}, {date_field_name}) 
    VALUES 
     (sde.next_rowid('{schema}', 'point_table_{sr}'), 'eeeefwe', TIMESTAMP '2019-05-15 15:53:53.522000' , 5654, TIMESTAMPTZ '2008-12-25 10:23:54+00' , null, ' 2017-06-26'),
    (sde.next_rowid('{schema}', 'point_table_{sr}'), 'ab#$%c', null , 12, TIMESTAMPTZ '2011-11-22 10:23:54-04' , ST_GEOMETRY('POINT(2712205.71100539 259685.27615705)', {sr}), ' 2005-01-01'),
    (sde.next_rowid('{schema}', 'point_table_{sr}'), 'd!@^&*?-=+ef', TIMESTAMP '2019-05-14 15:53:53.522000' , 1, null, ST_GEOMETRY('POINT(2672818.51681407 231921.15681663)', {sr}), ' 2015-03-01'),
     (sde.next_rowid('{schema}', 'point_table_{sr}'), 'fij()dcfwef', TIMESTAMP '2019-05-14 15:53:53.522000' , 2132134342, TIMESTAMPTZ '2014-04-11 10:23:54+05' , ST_GEOMETRY('POINT(2704440.74884506 251030.69241638)', {sr}), null),
     (sde.next_rowid('{schema}', 'point_table_{sr}'), 'po{}tato', TIMESTAMP '2019-05-14 15:53:53.522000' , 11, TIMESTAMPTZ '2021-08-23 10:23:54-02' , ST_GEOMETRY('POINT(2674410.98607007 233770.15508713)', {sr}), ' 2008-08-11'),
     (sde.next_rowid('{schema}', 'point_table_{sr}'), 'v[]im', TIMESTAMP '2019-05-14 15:53:53.522000' , 1353, TIMESTAMPTZ '2015-03-21 10:23:54-01' , ST_GEOMETRY('POINT(2694352.72374555 250468.93894671)', {sr}), ' 2005-09-07'),
     (sde.next_rowid('{schema}', 'point_table_{sr}'), 'he_llo', TIMESTAMP '2019-05-14 15:53:53.522000' , 49053, TIMESTAMPTZ '2020-06-12 10:23:54+03' , ST_GEOMETRY('POINT(2675096.08410931 231074.64210546)', {sr}), ' 2003-11-23'),
     (sde.next_rowid('{schema}', 'point_table_{sr}'), 'y"ea::h', TIMESTAMP '2018-03-30 15:10:06' , 123, TIMESTAMPTZ '2032-04-30 10:23:54-03' , ST_GEOMETRY('POINT(2694560.19708389 244942.81679688)', {sr}), ' 2020-04-01'),
     (sde.next_rowid('{schema}', 'point_table_{sr}'), 'qwe''qeqdqw', TIMESTAMP '2019-01-05 10:53:52' , 456, TIMESTAMPTZ '2018-12-25 10:23:54+00' , ST_GEOMETRY('POINT(2680866.32552156 241245.62340388)', {sr}), ' 2018-07-19'),
     (sde.next_rowid('{schema}', 'point_table_{sr}'), 'lmwefwe', TIMESTAMP '2019-05-14 15:53:53.522000' , 5654, TIMESTAMPTZ '2018-12-25 10:23:54+00' , ST_GEOMETRY('POINT(2692140.13630722 231409.33008438)', {sr}), ' 2017-06-26'),
     (sde.next_rowid('{schema}', 'point_table_{sr}'), 'lmwefwe', TIMESTAMP '2019-05-14 15:53:53.522000' , 5654, TIMESTAMPTZ '2018-12-25 10:23:54+00' , ST_GEOMETRY('POINT EMPTY', {sr}), ' 2017-06-26')'''.format(
        '''{}''',
        objectid_field_name=fields.get('object_id_field_name'),
        text_field_name=fields.get('text_field_name'),
        timestamp_field_name=fields.get('timestamp_field_name'),
        numeric_field_name=fields.get('numeric_field_name'),
        timezone_field_name=fields.get('timezone_field_name'),
        shape_field_name=fields.get('shape_field_name'),
        date_field_name=fields.get('date_field_name'),
        sr=srid, schema=schema)

    cursor = connection.cursor()
    cursor.execute('''truncate table {schema}.POINT_TABLE_{sr}'''.format(schema=schema, sr=srid))
    # cursor.execute('''
    #    ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'
    #                                NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF'
    #                                NLS_TIMESTAMP_TZ_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM'
    #     ''')
    cursor.execute(populate_table_stmt)
    connection.commit()

@pytest.fixture
def load_polygon_table(srid, postgis, schema):
    stmt = '''
    INSERT INTO {schema}.POLYGON_TABLE_{sr} ({shape_field_name}, {objectid_field_name}) 
    VALUES
    (SDE.ST_GEOMETRY('POLYGON(( 2697048.19400001 243967.35275000,2697046.11900000 244007.28925000,2697046.19599999 244038.87700000,2696984.16900000 244045.93900000,2697059.92400000 243874.43500000,2697048.19400001 243967.35275000))', {sr}), SDE.NEXT_ROWID('{schema}', 'polygon_table_{sr}')),
    (SDE.ST_GEOMETRY('POLYGON((2697048.19400001 243967.35275000,2697046.11900000 244007.28925000,2697046.19599999 244038.87700000,2696984.16900000 244045.93900000,2697059.92400000 243874.43500000,2697048.19400001 243967.35275000))', {sr}), SDE.NEXT_ROWID('{schema}', 'polygon_table_{sr}')),
    (SDE.ST_GEOMETRY('POLYGON((2697048.19400001 243967.35275000,2697046.11900000 244007.28925000,2697046.19599999 244038.87700000,2696984.16900000 244045.93900000,2697059.92400000 243874.43500000,2697048.19400001 243967.35275000))', {sr}), SDE.NEXT_ROWID('{schema}', 'polygon_table_{sr}')),
    (SDE.ST_GEOMETRY('POLYGON((2697048.19400001 243967.35275000,2697046.11900000 244007.28925000,2697046.19599999 244038.87700000,2696984.16900000 244045.93900000,2697059.92400000 243874.43500000,2697048.19400001 243967.35275000))', {sr}), SDE.NEXT_ROWID('{schema}', 'polygon_table_{sr}')),
    (SDE.ST_GEOMETRY('POLYGON((2697048.19400001 243967.35275000,2697046.11900000 244007.28925000,2697046.19599999 244038.87700000,2696984.16900000 244045.93900000,2697059.92400000 243874.43500000,2697048.19400001 243967.35275000))', {sr}), SDE.NEXT_ROWID('{schema}', 'polygon_table_{sr}')),
    (SDE.ST_GEOMETRY('POLYGON((2697048.19400001 243967.35275000,2697046.11900000 244007.28925000,2697046.19599999 244038.87700000,2696984.16900000 244045.93900000,2697059.92400000 243874.43500000,2697048.19400001 243967.35275000))', {sr}), SDE.NEXT_ROWID('{schema}', 'polygon_table_{sr}'))
    '''.format(schema=schema,
                sr=srid,
                objectid_field_name = fields.get('object_id_field_name'),
                shape_field_name = fields.get('shape_field_name')
    )
    connection = postgis.dbo
    cursor = connection.cursor()
    cursor.execute('''truncate table {schema}.POLYGON_TABLE_{sr}'''.format(schema=schema, sr=srid))
    cursor.execute(stmt)
    connection.commit()

@pytest.fixture
def csv_data():
    csv_data = etl.fromcsv(point_csv_dir).convert(['objectid','numericfield'], int)
    csv_data = etl.convert(csv_data,['timestamp','datefield','timezone'], lambda row: dt_parser.parse(row))
    csv_data = etl.convert(csv_data, 'datefield', lambda row: row.date())
    return csv_data


@pytest.fixture
def db_data(postgis,schema,srid):
    db_col = etl.frompostgis(dbo=postgis.dbo,table_name='{}.{}_{}'.format(schema,point_table_name,srid))
    return db_col

@pytest.fixture
def create_test_table_noid(postgis, schema,column_definition,srid):
    csv_data = etl.fromcsv(point_csv_dir).cutout('objectid')
    csv_data.topostgis(postgis.dbo, '{}.{}_{}'.format(schema,point_table_name,srid), column_definition_json=column_definition, from_srid=srid)

######################################   TESTS   ####################################################################

#read_data test
# compare csv data with postgres data using psycopg2
# def test_assert_point_table(postgis, csv_data, schema,srid, db_data,create_point_table):
#     csv_data = csv_data
#     csv_header = csv_data[0]
#     # read data using postgres
#     db_header = db_data[0]
#     i=1
#     # iterate through each row of data
#     for row in db_data[1:]:
#         # create dictionary for each row of data
#         csv_dict = dict(zip(csv_header, csv_data[i]))       # dictionary from csv data
#         pg_dict = dict(zip(db_header, row))                 # dictionary from postgres data
#         # iterate through each keys
#         for key in db_header:
#             # compare values from each key
#             if key==fields.get('shape_field_name'):
#                 csv_geom = csv_dict.get(key)
#                 pg_geom = pg_dict.get(key)
#                 if csv_geom == '':
#                     assert pg_geom is None
#                 else:
#                     pg_geom = geom_parser(str(pg_dict.get(key)), srid)
#                     csv_geom = geom_parser(str(csv_dict.get(key)), srid)
#                     assert csv_geom == pg_geom
#             elif key == fields.get('object_id_field_name'):
#                 continue
#             else:
#                 assert csv_dict.get(key) == pg_dict.get(key)
#         i=i+1
#------------------READING TESTS
#3
#compare csv data with postgres data using geopetl
def test_read_point_table(create_point_table, postgis,csv_data,db_data,srid):
    csv_header = csv_data[0]
    # read data using geopetl
    db_header = db_data[0]
    i=1
    # iterate through each row of data
    for row in db_data[1:]:
        # create dictionary for each row of data using same set of keys
        etl_dict = dict(zip(db_header, row))                # dictionary from etl data
        csv_dict = dict(zip(csv_header, csv_data[i]))       # dictionary from csv data
        # iterate through each keys
        for key in etl_dict:
            # assert shape field
            if key == fields.get('shape_field_name'):
                csv_geom = csv_dict.get(key)
                pg_geom = etl_dict.get(key)
                if csv_geom == '':
                    assert pg_geom is None
                else:
                    pg_geom = geom_parser(str(etl_dict.get(key)), srid)
                    csv_geom = geom_parser(str(csv_dict.get(key)), srid)
                    assert csv_geom == pg_geom
            elif key == fields.get('object_id_field_name'):
                continue
            # compare values from each key
            else:
                assert csv_dict.get(key) == etl_dict.get(key)
        i = i+1

def test_read_without_schema(db_data, postgis, csv_data, schema, srid):
    data = etl.frompostgis(dbo=postgis.dbo, table_name='{}.{}_{}'.format(schema,point_table_name, srid))
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
                if key == 'objectid':
                    continue
                # assert shape field
                #elif key == 'shape':
                elif key == fields.get('shape_field_name'):
                    db_val = etl_dict.get(key)
                    csv_val = csv_dict.get(key)
                    if csv_val == '':
                        assert db_val is None
                    else:
                        db_geom, db_coords = geom_parser(db_val, srid)
                        db_geom2, db_coords2 = geom_parser(csv_val, srid)
                        assert (db_geom == db_geom2 and db_coords == db_coords2)
                # compare values from each key
                else:
                    assert csv_dict.get(key) == etl_dict.get(key)
            i = i + 1

#4
def test_read_timestamp(csv_data, db_data):
    key = fields.get('timestamp_field_name')
    csv_col = csv_data[key]
    db_col = db_data[key]
    for i in range(len(db_col)):
         assert db_col[i] == csv_col[i]

#5
def test_read_numericfield(csv_data, db_data):
    key = fields.get('numeric_field_name')
    csv_col = csv_data[key]
    db_col = db_data[key]
    for i in range(len(db_col)):
         assert db_col[i] == csv_col[i]
#6
def test_read_datefield(csv_data, db_data):
    key = fields.get('date_field_name')
    csv_col = csv_data[key]
    db_col = db_data[key]
    for i in range(len(db_col)):
         assert db_col[i] == csv_col[i]
#7
def test_read_shape(csv_data, db_data,srid):
    key = fields.get('shape_field_name')
    csv_col = csv_data[key]
    db_col = db_data[key]
    for i in range(len(db_col)):
        db_val = db_col[i]
        csv_val = str(csv_col[i])
        if csv_val == '':
            assert db_val is None
        else:
            db_geom, db_coords = geom_parser(db_val,srid)
            csv_geom, csv_coords  = geom_parser(csv_val, srid)
            assert (db_geom == csv_geom and db_coords == csv_coords)

# #8
def test_assert_textfield(csv_data, db_data):
    key = fields.get('text_field_name')
    csv_col = csv_data[key]
    db_col = db_data[key]
    for i in range(len(db_col)):
         assert db_col[i] == csv_col[i]
# #9
def test_assert_timezone(csv_data, db_data):
    key = fields.get('timezone_field_name')
    csv_col = csv_data[key]
    db_col = db_data[key]
    for i in range(len(db_col)):
         assert db_col[i] == csv_col[i]


# #compare csv data with postgres data using geopetl
def test_reading_polygons(postgis, load_polygon_table,schema, csv_data, srid):
    csv_data = etl.fromcsv(polygon_csv_dir).convert(['objectid'], int)
    # list of column names
    keys = csv_data[0]

    # read data from test DB using petl
    db_data = etl.frompostgis(dbo=postgis.dbo, table_name='{}.{}_{}'.format(schema,polygon_table_name,srid))

    i=1
    # iterate through each row of data
    for row in db_data[1:]:
        # create dictionary for each row of data using same set of keys
        etl_dict = dict(zip(db_data[0], row))       # dictionary from etl data
        csv_dict = dict(zip(keys, csv_data[i]))     # dictionary from csv data
        # iterate through each keys
        for key in keys:
            # assert shape field
            if key== fields.get('shape_field_name'):
                pg_geom = etl_dict.get(key)
                csv_geom = csv_dict.get(key)
                if csv_geom == '':
                    assert pg_geom is None
                else:
                    db_geom, db_coords = geom_parser(pg_geom, srid)
                    db_geom2, db_coords2 = geom_parser(csv_geom, srid)
                    assert (db_geom == db_geom2 and db_coords == db_coords2)
            elif key ==fields.get('object_id_field_name'):
                continue
            # compare values from each key
            else:
                assert csv_dict.get(key) == etl_dict.get(key)
        i = i+1

# # assert DB data with itself

#WRITING TESTS
def test_write_without_schema(db_data, postgis, csv_data, schema, srid,column_definition):
    csv_data.topostgis(
        postgis.dbo,
        '{}_{}'.format(point_table_name, srid),
        from_srid=srid,
        column_definition_json=column_definition)

    data = etl.frompostgis(dbo=postgis.dbo, table_name='{}_{}'.format(point_table_name, srid))

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
                if key == fields.get('object_id_field_name'):
                    continue
                # assert shape field
                elif key == fields.get('shape_field_name'):
                    db_val = etl_dict.get(key)
                    csv_val = csv_dict.get(key)
                    if csv_val == '':
                        assert db_val is None
                    else:
                        db_geom, db_coords = geom_parser(db_val, srid)
                        db_geom2, db_coords2 = geom_parser(csv_val, srid)
                        assert (db_geom == db_geom2 and db_coords == db_coords2)
                # compare values from each key
                else:
                    assert csv_dict.get(key) == etl_dict.get(key)
            i = i + 1
#
# # # WRITING tests write using a string connection to db
def test_write_dsn_connection(csv_data,db, user, pw, host,postgis, column_definition,schema,create_point_table,srid):
    my_dsn = '''dbname={db} user={user} password={pw} host={host}'''.format(db=db,user=user,pw=pw,host=host)
    etl.topostgis(csv_data,
                  my_dsn,
                  '{}.{}_{}'.format(schema, point_table_name, srid),
                  from_srid=srid,
                  column_definition_json=column_definition)
    data = etl.frompostgis(dbo=postgis.dbo,table_name='{}.{}_{}'.format(schema,point_table_name,srid))
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
                if key == fields.get('object_id_field_name'):
                    continue
                    # assert csv_dict.get(key) == etl_dict.get(key)
                # assert shape field
                elif key == fields.get('shape_field_name'):
                    db_val = etl_dict.get(key)
                    csv_val = csv_dict.get(key)
                    if csv_val == '':
                        assert db_val is None
                    else:
                        db_geom, db_coords = geom_parser(db_val, srid)
                        db_geom2, db_coords2 = geom_parser(csv_val, srid)
                        assert (db_geom == db_geom2 and db_coords == db_coords2)
                else:
                    assert csv_dict.get(key) == etl_dict.get(key)
            i = i + 1

# # # WRITING TEST load csv data to postgressde db without an objectid field using geopetl and assert data
def test_write_data_no_id(csv_data, db_data,srid, postgis,schema, column_definition):
    data = etl.fromcsv(point_csv_dir).cutout(fields.get('object_id_field_name'))
    data.topostgis(
                  postgis.dbo,
                  '{}.{}_{}'.format(schema, point_table_name, srid),
                  from_srid=srid,
                  column_definition_json=column_definition)
    # list of column names
    i=1
    # iterate through each row of data
    for row in db_data[1:]:
        # create dictionary for each row of data using same set of keys
        csv_dict = dict(zip(csv_data[0], csv_data[i]))         # dictionary from csv data
        db_dict = dict(zip(db_data[0], row))        # dictionary from postgres data
        object_id_field_name = fields.get('object_id_field_name')
        for key in csv_data[0]:
            if key == object_id_field_name and object_id_field_name in csv_data[0] and object_id_field_name in db_data[0]:
                assert (db_dict.get('objectid') is not None)
            elif key == fields.get('shape_field_name'):
                db_val = db_dict.get(key)
                csv_val = str(csv_dict.get(key))
                if csv_val == '':
                    assert db_val is None
                else:
                    db_geom, db_coords = geom_parser(db_val, srid)
                    csv_geom, csv_coords = geom_parser(csv_val, srid)
                    assert (db_geom == csv_geom and db_coords == csv_coords)
            else:
                assert db_dict.get(key) == csv_dict.get(key)
        i=i+1





# debug!!--------
# # # # assert DB data with itself
# def test_with_types(db_data, postgis, column_definition, schema, srid):
#     # read data from db
#     data1 = db_data
#     #load to second test table
#     etl.topostgis(db_data, postgis.dbo, '{}.{}_{}_2'.format(schema,point_table_name,srid), column_definition_json=column_definition,  from_srid=srid)
#     #extract from second test table
#     data2 = etl.frompostgis(dbo=postgis.dbo,table_name='{}.{}_{}_2'.format(schema,point_table_name,srid))
#     i = 1
#     # iterate through each row of DB data
#     for row in data1[1:]:
#         # create dictionary with header and each row of data
#         db_dict1 = dict(zip(data1[0], data1[i]))
#         db_dict2 = dict(zip(data2[0], data2[i]))
#         # iterate through each keys
#         for key in db_data[0]:
#             # assert shape field
#             if key == fields.get('object_id_field_name'):
#                 continue
#             if key == fields.get('shape'):
#                 # assert values from each key
#                 db_val = db_dict1.get(key)
#                 csv_val = db_dict2.get(key)
#                 if csv_val == '':
#                     assert db_val is None
#                 else:
#                     db_geom, db_coords = geom_parser(db_val, srid)
#                     db_geom2, db_coords2 = geom_parser(csv_val, srid)
#                     assert (db_geom == db_geom2 and db_coords == db_coords2)
#             else:
#                 assert db_dict1.get(key) == db_dict2.get(key)
#         i = i + 1


# # # # write using a string connection to db
def test_write_dsn_connection(csv_data,db, user, pw, host,postgis, column_definition,schema,srid):
    my_dsn = '''dbname={db} user={user} password={pw} host={host}'''.format(db=db,user=user,pw=pw,host=host)
    etl.topostgis(csv_data,
                  my_dsn,
                  '{}.{}_{}'.format(schema,point_table_name,srid),
                  )
    data = etl.frompostgis(dbo=postgis.dbo,
                           table_name='{}.{}_{}'.format(schema,point_table_name,srid),
                           #from_srid =srid
                           )
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
                if key == fields.get('object_id_field_name'):
                    continue
                # assert shape field
                if key == fields.get('shape_field_name'):
                    db_val = etl_dict.get(key)
                    csv_val = csv_dict.get(key)
                    if csv_val == '':
                        assert db_val is None
                    else:
                        db_geom, db_coords = geom_parser(db_val, srid)
                        db_geom2, db_coords2 = geom_parser(csv_val, srid)
                        assert (db_geom == db_geom2 and db_coords == db_coords2)
                # compare values from each key
                else:
                    assert csv_dict.get(key) == etl_dict.get(key)
            i = i + 1
# #
# #


# WRITING TEST?
def test_null_times(postgis, csv_data, schema, column_definition, srid):
    csv_data['timestamp'] = ''
    csv_data['timezone'] = ''
    csv_data['datefield'] = ''
    keys = csv_data[0]
    tb = postgis.table('{}.{}_{}'.format(schema, point_table_name, srid))
    csv_data.topostgis(postgis.dbo, '{}.{}_{}'.format(schema,point_table_name,srid), column_definition_json=column_definition,
                       from_srid=srid)
    db_data = etl.frompostgis(dbo=postgis.dbo, table_name='{}.{}_{}'.format(schema, point_table_name, srid))

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
                pg_geom = etl_dict.get(key)
                csv_geom = csv_dict.get(key)
                if csv_geom == '':
                    assert pg_geom is None
                else:
                    db_geom, db_coords = geom_parser(pg_geom, srid)
                    db_geom2, db_coords2 = geom_parser(csv_geom, srid)
                    assert (db_geom == db_geom2 and db_coords == db_coords2)
            elif key == 'objectid':
                continue
            # compare values from each key
            else:
                assert csv_dict.get(key) == etl_dict.get(key)
        i = i + 1



