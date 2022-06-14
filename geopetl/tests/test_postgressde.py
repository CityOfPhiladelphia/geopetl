import pytest
import petl as etl
from geopetl.postgis import PostgisDatabase
import psycopg2
# from pytz import timezone
# import csv
from dateutil import parser as dt_parser
from tests_config import geom_parser, line_csv_dir, line_table_name, polygon_csv_dir, line_table_name,polygon_table_name, point_table_name, point_csv_dir,fields, multipolygon_table_name, multipolygon_csv_dir


############################################# FIXTURES ################################################################

# return postgres database object
@pytest.fixture
def postgis(db, user, pw, host):
    # create connection string
    dsn = "host={0} dbname={1} user={2} password={3}".format(host,db,user,pw)
    # create & return geopetl postgis object
    postgis_db = PostgisDatabase(dsn)
    return postgis_db

# create new table and write csv staging data to it
@pytest.fixture
def load_point_table(postgis,schema, srid):
    # write staging data to test table using sql query
    connection = postgis.dbo
    populate_table_stmt = ''' INSERT INTO {schema}.{point_table_name}_{sr} ({objectid_field_name}, {text_field_name}, {timestamp_field_name}, {numeric_field_name}, {timezone_field_name}, {shape_field_name}, {date_field_name}) 
    VALUES 
     (sde.next_rowid('{schema}', 'point_table_{sr}'), '', NULL , NULL, NULL, null, NULL),
     (sde.next_rowid('{schema}', 'point_table_{sr}'), 'ab#$%c','2019-05-15 15:53:53.522000', 12, TIMESTAMPTZ '2011-11-22 10:23:54-04' , ST_GEOMETRY('POINT(2712205.71100539 259685.27615705)', {sr}), ' 2005-01-01'),
     (sde.next_rowid('{schema}', 'point_table_{sr}'), 'd!@^&*?-=+ef', TIMESTAMP '2019-05-14 15:53:53.522000' , 1, null, ST_GEOMETRY('POINT(2672818.51681407 231921.15681663)', {sr}), ' 2015-03-01'),
     (sde.next_rowid('{schema}', 'point_table_{sr}'), 'fij()dcfwef', TIMESTAMP '2019-05-14 15:53:53.522000' , 2132134342, TIMESTAMPTZ '2014-04-11 10:23:54+05' , ST_GEOMETRY('POINT(2704440.74884506 251030.69241638)', {sr}), null),
     (sde.next_rowid('{schema}', 'point_table_{sr}'), 'po{}tato', TIMESTAMP '2019-05-14 15:53:53.522000' , 11, TIMESTAMPTZ '2021-08-23 10:23:54-02' , ST_GEOMETRY('POINT(2674410.98607007 233770.15508713)', {sr}), ' 2008-08-11'),
     (sde.next_rowid('{schema}', 'point_table_{sr}'), 'v[]im', TIMESTAMP '2019-05-14 15:53:53.522000' , 1353, TIMESTAMPTZ '2015-03-21 10:23:54-01' , ST_GEOMETRY('POINT(2694352.72374555 250468.93894671)', {sr}), ' 2005-09-07'),
     (sde.next_rowid('{schema}', 'point_table_{sr}'), 'he_llo', TIMESTAMP '2019-05-14 15:53:53.522000' , 49053, TIMESTAMPTZ '2020-06-12 10:23:54+03' , ST_GEOMETRY('POINT(2675096.08410931 231074.64210546)', {sr}), ' 2003-11-23'),
     (sde.next_rowid('{schema}', 'point_table_{sr}'), 'y"ea::h', TIMESTAMP '2018-03-30 15:10:06' , 123, TIMESTAMPTZ '2032-04-30 10:23:54-03' , ST_GEOMETRY('POINT(2694560.19708389 244942.81679688)', {sr}), ' 2020-04-01'),
     (sde.next_rowid('{schema}', 'point_table_{sr}'), 'qwe''qeqdqw', TIMESTAMP '2019-01-05 10:53:52' , 456, TIMESTAMPTZ '2018-12-25 10:23:54+00' , ST_GEOMETRY('POINT(2680866.32552156 241245.62340388)', {sr}), ' 2018-07-19'),
     (sde.next_rowid('{schema}', 'point_table_{sr}'), 'lmwefwe', TIMESTAMP '2019-05-14 15:53:53.522000' , 5654, TIMESTAMPTZ '2018-12-25 10:23:54+00' , ST_GEOMETRY('POINT(2692140.13630722 231409.33008438)', {sr}), ' 2017-06-26'),
     (sde.next_rowid('{schema}', 'point_table_{sr}'), '', TIMESTAMP '2019-05-14 15:53:53.522000' , 5654, TIMESTAMPTZ '2018-12-25 10:23:54+00' , ST_GEOMETRY('POINT EMPTY', {sr}), ' 2017-06-26')'''.format(
        '''{}''',
        objectid_field_name=fields.get('object_id_field_name'),
        text_field_name=fields.get('text_field_name'),
        timestamp_field_name=fields.get('timestamp_field_name'),
        numeric_field_name=fields.get('numeric_field_name'),
        timezone_field_name=fields.get('timezone_field_name'),
        shape_field_name=fields.get('shape_field_name'),
        date_field_name=fields.get('date_field_name'),
        sr=srid, schema=schema,
        point_table_name= point_table_name )
    cursor = connection.cursor()
    cursor.execute('''truncate table {schema}.{point_table_name}_{sr}'''.format(schema=schema,point_table_name=point_table_name, sr=srid))
    cursor.execute(populate_table_stmt)
    connection.commit()

@pytest.fixture
def load_polygon_table(srid, postgis, schema):
    stmt = '''
    INSERT INTO {schema}.{polygon_table_name}_{sr} ({shape_field_name}, {objectid_field_name}) 
    VALUES
    (
     NULL, SDE.NEXT_ROWID('{schema}', 'polygon_table_{sr}')),
    (SDE.ST_GEOMETRY('POLYGON((2697048.19400001 243967.35275000,2697046.11900000 244007.28925000,2697046.19599999 244038.87700000,2696984.16900000 244045.93900000,2697059.92400000 243874.43500000,2697048.19400001 243967.35275000))', {sr}),
     SDE.NEXT_ROWID('{schema}', 'polygon_table_{sr}')),
    (SDE.ST_GEOMETRY('POLYGON((2697048.19400001 243967.35275000,2697046.11900000 244007.28925000,2697046.19599999 244038.87700000,2696984.16900000 244045.93900000,2697059.92400000 243874.43500000,2697048.19400001 243967.35275000))', {sr}), SDE.NEXT_ROWID('{schema}', 'polygon_table_{sr}')),
    (SDE.ST_GEOMETRY('POLYGON((2697048.19400001 243967.35275000,2697046.11900000 244007.28925000,2697046.19599999 244038.87700000,2696984.16900000 244045.93900000,2697059.92400000 243874.43500000,2697048.19400001 243967.35275000))', {sr}), SDE.NEXT_ROWID('{schema}', 'polygon_table_{sr}')),
    (SDE.ST_GEOMETRY('POLYGON((2697048.19400001 243967.35275000,2697046.11900000 244007.28925000,2697046.19599999 244038.87700000,2696984.16900000 244045.93900000,2697059.92400000 243874.43500000,2697048.19400001 243967.35275000))', {sr}), SDE.NEXT_ROWID('{schema}', 'polygon_table_{sr}')),
    (SDE.ST_GEOMETRY('POLYGON((2697048.19400001 243967.35275000,2697046.11900000 244007.28925000,2697046.19599999 244038.87700000,2696984.16900000 244045.93900000,2697059.92400000 243874.43500000,2697048.19400001 243967.35275000))', {sr}), SDE.NEXT_ROWID('{schema}', 'polygon_table_{sr}'))
    '''.format(schema=schema,
                sr=srid,
               polygon_table_name=polygon_table_name,
                objectid_field_name = fields.get('object_id_field_name'),
                shape_field_name = fields.get('shape_field_name')
    )
    connection = postgis.dbo
    cursor = connection.cursor()
    cursor.execute('''truncate table {schema}.{polygon_table_name}_{srid}'''.format(schema=schema, polygon_table_name=polygon_table_name,srid=srid))
    cursor.execute(stmt)
    connection.commit()

@pytest.fixture
def load_line_table(srid, postgis, schema):
    stmt = '''	
    INSERT INTO {schema}.{line_table_name}_{sr} ({shape_field_name}, {objectid_field_name}) 
    VALUES 
    (NULL, SDE.NEXT_ROWID('{schema}', 'line_table_{sr}')),
    (SDE.ST_GEOMETRY('LINESTRING(2679640.41975001 259205.68799999, 2679610.90800001 259142.53425001)', {sr}), SDE.NEXT_ROWID('{schema}', 'line_table_{sr}')),
    (SDE.ST_GEOMETRY('LINESTRING(2679640.41975001 259205.68799999, 2679610.90800001 259142.53425001)', {sr}), SDE.NEXT_ROWID('{schema}', 'line_table_{sr}')),
    (SDE.ST_GEOMETRY('LINESTRING(2679640.41975001 259205.68799999, 2679610.90800001 259142.53425001)', {sr}), SDE.NEXT_ROWID('{schema}', 'line_table_{sr}')),
    (SDE.ST_GEOMETRY('LINESTRING(2679640.41975001 259205.68799999, 2679610.90800001 259142.53425001)', {sr}), SDE.NEXT_ROWID('{schema}', 'line_table_{sr}')),
    (SDE.ST_GEOMETRY('LINESTRING(2679640.41975001 259205.68799999, 2679610.90800001 259142.53425001)', {sr}), SDE.NEXT_ROWID('{schema}', 'line_table_{sr}'))
    '''.format(schema=schema,
               sr=srid,
               line_table_name=line_table_name,
                objectid_field_name=fields.get('object_id_field_name'),
                shape_field_name=fields.get('shape_field_name'))
    connection = postgis.dbo
    cursor = connection.cursor()
    cursor.execute('''truncate table {schema}.{line_table_name}_{sr}'''.format(schema=schema,line_table_name=line_table_name, sr=srid))
    cursor.execute(stmt)
    connection.commit()

@pytest.fixture
def load_multipolygon_table(srid, postgis, schema):
    stmt = '''INSERT INTO {multipolygon_table_name}_{srid} ({objectid_field_name}, {shape_field_name})
            VALUES 
            (SDE.NEXT_ROWID('{schema}', '{multipolygon_table_name}_{srid}'),
            sde.st_multipolygon ('multipolygon ((
            (2697048.194000 243967.352750, 2697049.194000 243968.352750, 2697050.194000 243968.352750, 2697048.194000 243967.352750),
            (2697046.119000 244007.289250, 2697048.119000 244009.289250, 2697046.119000 244007.289250),
            (2697059.924000 243874.435000, 2697057.924000 243872.435000, 2697058.924000 243871.435000, 2697059.924000 243872.435000,2697059.924000 243874.435000)
            ))', {srid})),

            (SDE.NEXT_ROWID('{schema}', '{multipolygon_table_name}_{srid}'),
            sde.st_multipolygon ('multipolygon ((
            (2697048.094000 243967.352750, 2697049.194000 243968.352750, 2697050.194000 243968.352750, 2697048.094000 243967.352750),
            (2697046.019000 244007.289250, 2697048.119000 244009.289250, 2697046.019000 244007.289250),
            (2697059.824000 243874.435000, 2697057.924000 243872.435000, 2697058.924000 243871.435000, 2697059.924000 243872.435000,2697059.824000 243874.435000)
            ))', {srid})),

            (SDE.NEXT_ROWID('{schema}', '{multipolygon_table_name}_{srid}'),
            sde.st_multipolygon ('multipolygon ((
            (2697048.294000 243967.352750, 2697049.194000 243968.352750, 2697050.194000 243968.352750, 2697048.294000 243967.352750),
            (2697046.219000 244007.289250, 2697048.119000 244009.289250, 2697046.219000 244007.289250),
            (2697059.984000 243874.435000, 2697057.924000 243872.435000, 2697058.924000 243871.435000, 2697059.924000 243872.435000,2697059.984000 243874.435000)
            ))', {srid})),

            (SDE.NEXT_ROWID('{schema}', '{multipolygon_table_name}_{srid}'),
            sde.st_multipolygon ('multipolygon ((
            (2697048.194000 243967.452750, 2697049.194000 243968.352750, 2697050.194000 243968.352750, 2697048.194000 243967.452750),
            (2697046.119000 244007.389250, 2697048.119000 244009.289250, 2697046.119000 244007.389250),
            (2697059.924000 243874.535000, 2697057.924000 243872.435000, 2697058.924000 243871.435000, 2697059.924000 243872.435000,2697059.924000 243874.535000)
            ))', {srid}))'''.format(schema=schema,
                                    multipolygon_table_name=multipolygon_table_name,
                                    objectid_field_name=fields.get('object_id_field_name'),
                                    shape_field_name=fields.get('shape_field_name'),
                                    srid=srid)

    connection = postgis.dbo
    cursor = connection.cursor()
    cursor.execute('''truncate table {schema}.{table_name}_{srid}'''.format(schema=schema, srid=srid,table_name=multipolygon_table_name))
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
def create_test_table_noid(postgis, schema,srid):
    csv_data = etl.fromcsv(point_csv_dir).cutout(fields.get('object_id_field_name'))
    csv_data.topostgis(postgis.dbo, '{}.{}_{}'.format(schema,point_table_name,srid), from_srid=srid)


# assert
def assert_data_method(csv_data1, db_data1, srid1, field=None):
    keys = csv_data1[0]
    try:
        db_header = [column[0] for column in db_data1.description]
        db_data1 = db_data1.fetchall()
        i=0
    except:
        db_header = db_data1[0]
        i=1
    for row in csv_data1[1:]:
        etl_dict = dict(zip(db_header, db_data1[i]))  # dictionary from etl data
        csv_dict = dict(zip(csv_data1[0], row))  # dictionary from csv data
        if field:
            keys = list(field)
        for key in keys:
            csv_val = csv_dict.get(key)
            db_val = etl_dict.get(key)
            # assert shape field
            if key == fields.get('shape_field_name'):
                if csv_val == '':
                    assert db_val is None
                else:
                    pg_geom = geom_parser(str(csv_val), srid1)
                    csv_geom = geom_parser(str(db_val), srid1)
                    assert csv_geom == pg_geom
            elif key == fields.get('object_id_field_name'):
                continue
            elif key == fields.get('timezone_field_name'):
                if csv_val == None or csv_val == '':
                    assert db_val is None
                else:
                    try:
                        db_val = dt_parser.parse(db_val)
                    except:
                        db_val = db_val
                    try:
                        csv_val = dt_parser.parse(csv_val)
                    except:
                        csv_val = csv_val
                    assert db_val == csv_val
            # compare values from each key
            else:
                assert csv_val == db_val
        i=i+1

######################################   TESTS   ####################################################################

#------------------READING TESTS
#compare csv data with postgres data using geopetl
def test_reading_point_table(load_point_table, postgis,csv_data,db_data,srid):
    assert_data_method(csv_data, db_data, srid)

def test_reading_without_schema(postgis, csv_data, schema, srid):
    data = etl.frompostgis(dbo=postgis.dbo, table_name='{}.{}_{}'.format(schema,point_table_name, srid))
    assert_data_method(csv_data, data, srid)

#4
def test_reading_timestamp(csv_data, db_data,srid):
    key = fields.get('timestamp_field_name')
    assert_data_method(csv_data,db_data, srid, field=key)

#5
def test_reading_numericfield(csv_data, db_data,srid):
    key = fields.get('numeric_field_name')
    db_data1 = db_data
    csv_data1 = csv_data
    assert_data_method(csv_data1, db_data1, srid, field=key)
#6
def test_reading_datefield(csv_data, db_data, srid):
    key = fields.get('date_field_name')
    db_data1 = db_data
    csv_data1 = csv_data
    assert_data_method(csv_data1, db_data1, srid, field=key)

#7
def test_reading_shape(csv_data, db_data,srid):
    key = fields.get('shape_field_name')
    db_data1 = db_data
    csv_data1 = csv_data
    assert_data_method(csv_data1, db_data1, srid, field=key)

# #8
def test_reading_textfield(csv_data, db_data,srid):
    key = fields.get('text_field_name')
    db_data1 = db_data
    csv_data1 = csv_data
    assert_data_method(csv_data1, db_data1, srid, field=key)
# #9
def test_reading_timezone(csv_data, db_data,srid):
    key = fields.get('timezone_field_name')
    db_data1 = db_data
    csv_data1 = csv_data
    assert_data_method(csv_data1, db_data1, srid, field=key)

# #compare csv data with postgres data using geopetl
def test_reading_polygon_table(postgis, load_polygon_table,schema, srid):
    csv_data = etl.fromcsv(polygon_csv_dir)
    # read data from test DB using petl
    db_data1 = etl.frompostgis(dbo=postgis.dbo, table_name='{}.{}_{}'.format(schema,polygon_table_name,srid))
    assert_data_method(csv_data, db_data1, srid)

def test_reading_line_table(postgis, load_line_table,schema, srid):
    csv_data = etl.fromcsv(line_csv_dir)
    # read data from test DB using petl
    db_data1 = etl.frompostgis(dbo=postgis.dbo, table_name='{}.{}_{}'.format(schema,line_table_name,srid))
    assert_data_method(csv_data, db_data1, srid)

def test_reading_multipolygon(postgis, load_multipolygon_table, schema, srid):
    csv_data = etl.fromcsv(multipolygon_csv_dir)
    # read data from test DB using petl
    db_data1 = etl.frompostgis(dbo=postgis.dbo, table_name='{}.{}_{}'.format(schema, multipolygon_table_name, srid))
    assert_data_method(csv_data, db_data1, srid)


#------------------WRITING TESTS
def test_write_without_schema(db_data, postgis, csv_data, schema, srid):
    connection = postgis.dbo
    csv_data.topostgis(
            connection,
            '{}_{}'.format(point_table_name, srid),
            from_srid=srid
    )
    cursor = connection.cursor()
    stmt = '''
            select {objectid_field_name},{text_field_name},{numeric_field_name},{timestamp_field_name},{date_field_name},
            to_char({timezone_field_name}, 'YYYY-MM-DD HH24:MI:SS.FFTZH:TZM') as {timezone_field_name},
            sde.st_astext({shape_field_name}) as {shape_field_name} from {schema}.{point_table_name}_{srid}'''.format(
        schema=schema,
        point_table_name=point_table_name,
        srid=srid,
        objectid_field_name=fields.get('object_id_field_name'),
        text_field_name=fields.get('text_field_name'),
        numeric_field_name=fields.get('numeric_field_name'),
        timestamp_field_name=fields.get('timestamp_field_name'),
        date_field_name=fields.get('date_field_name'),
        shape_field_name=fields.get('shape_field_name'),
        timezone_field_name=fields.get('timezone_field_name')
    )
    cursor.execute(stmt)
    assert_data_method(csv_data, cursor,srid)

# # # WRITING tests write using a string connection to db
def test_write_dsn_connection(csv_data,db, user, pw, host,postgis,schema,srid):
    my_dsn = '''dbname={db} user={user} password={pw} host={host}'''.format(db=db,user=user,pw=pw,host=host)
    etl.topostgis(csv_data,
                  my_dsn,
                  '{}.{}_{}'.format(schema, point_table_name, srid),
                  from_srid=srid)
    connection = postgis.dbo
    cursor = connection.cursor()
    stmt = '''
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
    )
    cursor.execute(stmt)
    assert_data_method(csv_data, cursor, srid)

# # # WRITING TEST load csv data to postgressde db without an objectid field using geopetl and assert data
def test_write_data_no_id(csv_data, db_data,srid, postgis,schema):
    data = etl.fromcsv(point_csv_dir).cutout(fields.get('object_id_field_name'))
    data.topostgis(
                  postgis.dbo,
                  '{}.{}_{}'.format(schema, point_table_name, srid),
                  from_srid=srid)
    connection = postgis.dbo
    cursor = connection.cursor()
    stmt = '''
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
        )
    cursor.execute(stmt)
    assert_data_method(csv_data, cursor, srid)


# WRITING TEST?
def test_null_times(postgis, csv_data, schema, srid):
    csv_data['timestamp'] = ''
    csv_data['timezone'] = ''
    csv_data['datefield'] = ''
    csv_data.topostgis(postgis.dbo, '{}.{}_{}'.format(schema,point_table_name,srid),
                       from_srid=srid)
    connection = postgis.dbo
    cursor = connection.cursor()
    stmt = '''
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
    )
    cursor.execute(stmt)
    assert_data_method(csv_data, cursor, srid)


def test_polygon_assertion_write(postgis, schema, srid):
    csv_data = etl.fromcsv(polygon_csv_dir)
    csv_data.topostgis(postgis.dbo, '{}.{}_{}'.format(schema, polygon_table_name, srid),from_srid=srid)
    # read data from test DB using petl
    stmt = '''select {objectid_field_name}, SDE.ST_AsText({shape_field_name}) as {shape_field_name} from {}.{}_{}'''.format(
        schema,
        polygon_table_name,
        srid,
        objectid_field_name = fields.get('object_id_field_name'),
        shape_field_name = fields.get('shape_field_name'))
    cursor = postgis.dbo.cursor()
    cursor.execute(stmt)
    assert_data_method(csv_data, cursor, srid)

def test_line_assertion_write(postgis, schema,srid):
    csv_data = etl.fromcsv(line_csv_dir)
    csv_data.topostgis(postgis.dbo, '{}.{}_{}'.format(schema, line_table_name, srid), from_srid=srid)
    # read data from test DB
    stmt = '''select {objectid_field_name}, SDE.ST_AsText({shape_field_name}) as {shape_field_name} from {}.{}_{}'''.format(
        schema,
        line_table_name,
        srid,
        objectid_field_name = fields.get('object_id_field_name'),
        shape_field_name = fields.get('shape_field_name')
    )
    cursor = postgis.dbo.cursor()
    cursor.execute(stmt)
    assert_data_method(csv_data, cursor, srid)

def test_multipolygon_assertion_write(postgis, load_multipolygon_table, schema, srid):
    csv_data = etl.fromcsv(multipolygon_csv_dir)
    csv_data.topostgis(postgis.dbo, '{}.{}_{}'.format(schema, multipolygon_table_name, srid), from_srid=srid)
    stmt = '''select {objectid_field_name}, SDE.ST_AsText({shape_field_name}) as {shape_field_name} from {}.{}_{}'''.format(
        schema,
        multipolygon_table_name,
        srid,
        objectid_field_name=fields.get('object_id_field_name'),
        shape_field_name=fields.get('shape_field_name')
    )
    cursor = postgis.dbo.cursor()
    cursor.execute(stmt)
    assert_data_method(csv_data, cursor, srid)

# read and write
# # # # assert DB data with itself
def test_with_types(db_data, postgis, schema, srid):
    # read data from db
    data1 = db_data
    etl.topostgis(data1,postgis.dbo, '{}.{}_{}_2'.format(schema, point_table_name, srid), from_srid=srid)
    data2 = etl.frompostgis(dbo=postgis.dbo,
                            table_name='{}.{}_{}_2'.format(schema, point_table_name, srid))
    assert_data_method(data1, data2, srid)

