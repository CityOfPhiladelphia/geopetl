import pytest
import petl as etl
from geopetl.postgis import PostgisDatabase
import psycopg2
from pytz import timezone
import csv
from dateutil import parser as dt_parser
from tests_config import geom_parser, line_csv_dir, line_table_name,line_column_definition,\
    polygon_csv_dir, polygon_table_name, polygon_column_definition, \
    point_table_name, point_csv_dir, point_column_definition,fields, multipolygon_table_name, multipolygon_csv_dir,multipolygon_column_definition


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
def load_non_geom_table(postgis, schema):
    create = '''DROP TABLE IF EXISTS {schema}.{point_table_name}_ng;
    CREATE TABLE {schema}.{point_table_name}_ng
    (
    objectid numeric,
    textfield text,
    "timestamp" timestamp without time zone,
    numericfield numeric,
    timezone timestamp with time zone,
    datefield date,
    booleanfield boolean
    );

    INSERT INTO {schema}.{point_table_name}_ng
    ({objectid_field_name}, textfield, timestamp, numericfield, timezone, datefield, booleanfield)
     VALUES
    (1,NULL,NULL,NULL,NULL, NULL,NULL),
    (2, 'ab#$%c', '2019-05-15 15:53:53.522000', 12, TIMESTAMPTZ '2011-11-22 10:23:54-04', ' 2005-01-01','true'),
    (3, 'd!@^&*?-=+ef', TIMESTAMP '2019-05-14 15:53:53.522000' , 1, NULL, ' 2015-03-01','true'),
    (4, 'fij()dcfwef', TIMESTAMP '2019-05-14 15:53:53.522000' , 2132134342, TIMESTAMPTZ '2014-04-11 10:23:54+05' , NULL,'true'), 
    (5, 'po{}tato', TIMESTAMP '2019-05-14 15:53:53.522000' , 0, TIMESTAMPTZ '2021-08-23 10:23:54-02' , '2008-08-11','true'),
    (6, 'v[]im', TIMESTAMP '2019-05-14 15:53:53.522000' , 1353, TIMESTAMPTZ '2015-03-21 10:23:54-01' , '2005-09-07','true'), 
    (7, 'he_llo', TIMESTAMP '2019-05-14 15:53:53.522000' , 49053, TIMESTAMPTZ '2020-06-12 10:23:54+03' , ' 2003-11-23','true'), 
    (8, 'y"ea::h', TIMESTAMP '2018-03-30 15:10:06' , -123, TIMESTAMPTZ '2032-04-30 10:23:54-03' , '2020-04-01','false'),
    (9, 'qwe''qeqdqw', TIMESTAMP '2019-01-05 10:53:52' , 456, TIMESTAMPTZ '2018-12-25 10:23:54+00' , '2018-07-19','false'), 
    (10, 'lmwefwe', TIMESTAMP '2019-05-14 15:53:53.522000' , 5654, TIMESTAMPTZ '2018-12-25 10:23:54+00' ,'2017-06-26','false'), 
    (11, NULL, TIMESTAMP '2019-05-14 15:53:53.522000' , 5654, TIMESTAMPTZ '2018-12-25 10:23:54+00' , '2017-06-26','false')'''.format('''{}''',
        objectid_field_name=fields.get('object_id_field_name'),
        schema=schema,
        point_table_name=point_table_name)
    connection = postgis.dbo
    cursor = connection.cursor()
    cursor.execute(create)
    connection.commit()

# create new table and write csv staging data to it
@pytest.fixture
def load_point_table(postgis,schema, srid):
    # write staging data to test table using oracle query
    connection = postgis.dbo
    mview_stmt= '''DROP MATERIALIZED VIEW IF EXISTS {schema}.point_table_{srid}_view;'''
    populate_table_stmt ='''
    DROP MATERIALIZED VIEW IF EXISTS {schema}.point_table_{srid}_view;
    DROP TABLE IF EXISTS {schema}.{point_table_name}_{srid};
    CREATE TABLE {schema}.{point_table_name}_{srid}
    (
    objectid numeric,
    textfield text,
    "timestamp" timestamp without time zone,
    numericfield numeric,
    timezone timestamp with time zone,
    shape geometry(Point,{srid}),
    datefield date,
    booleanfield boolean
    );
    
    INSERT INTO {schema}.{point_table_name}_{srid} 
    ({objectid_field_name}, textfield, timestamp, numericfield, timezone, {shape_field_name}, datefield, booleanfield)
     VALUES
    (1,NULL,NULL,NULL,NULL, NULL,NULL,NULL),
    (2, 'ab#$%c', '2019-05-15 15:53:53.522000', 12, TIMESTAMPTZ '2011-11-22 10:23:54-04' , ST_GeomFromText('POINT(2712205.71100539 259685.27615705)', {srid}), ' 2005-01-01', 'true'),
    (3, 'd!@^&*?-=+ef', TIMESTAMP '2019-05-14 15:53:53.522000' , 1, NULL, ST_GeomFromText('POINT(2672818.51681407 231921.15681663)', {srid}), ' 2015-03-01','true'),
    (4, 'fij()dcfwef', TIMESTAMP '2019-05-14 15:53:53.522000' , 2132134342, TIMESTAMPTZ '2014-04-11 10:23:54+05' , ST_GeomFromText('POINT(2704440.74884506 251030.69241638)', {srid}), NULL,'true'), 
    (5, 'po{}tato', TIMESTAMP '2019-05-14 15:53:53.522000' , 0, TIMESTAMPTZ '2021-08-23 10:23:54-02' , ST_GeomFromText('POINT(2674410.98607007 233770.15508713)', {srid}), ' 2008-08-11','true'),
    (6, 'v[]im', TIMESTAMP '2019-05-14 15:53:53.522000' , 1353, TIMESTAMPTZ '2015-03-21 10:23:54-01' , ST_GeomFromText('POINT(2694352.72374555 250468.93894671)', {srid}), ' 2005-09-07','true'), 
    (7, 'he_llo', TIMESTAMP '2019-05-14 15:53:53.522000' , 49053, TIMESTAMPTZ '2020-06-12 10:23:54+03' , ST_GeomFromText('POINT(2675096.08410931 231074.64210546)', {srid}), ' 2003-11-23','false'), 
    (8, 'y"ea::h', TIMESTAMP '2018-03-30 15:10:06' , -123, TIMESTAMPTZ '2032-04-30 10:23:54-03' , ST_GeomFromText('POINT(2694560.19708389 244942.81679688)', {srid}), ' 2020-04-01','false'),
    (9, 'qwe''qeqdqw', TIMESTAMP '2019-01-05 10:53:52' , 456, TIMESTAMPTZ '2018-12-25 10:23:54+00' , ST_GeomFromText('POINT(2680866.32552156 241245.62340388)', {srid}), ' 2018-07-19','false'), 
    (10, 'lmwefwe', TIMESTAMP '2019-05-14 15:53:53.522000' , 5654, TIMESTAMPTZ '2018-12-25 10:23:54+00' , ST_GeomFromText('POINT(2692140.13630722 231409.33008438)', {srid}), ' 2017-06-26','false'), 
    (11, NULL, TIMESTAMP '2019-05-14 15:53:53.522000' , 5654, TIMESTAMPTZ '2018-12-25 10:23:54+00' , ST_GeomFromText('POINT EMPTY', {srid}), ' 2017-06-26','false')'''.format(
        '''{}''',
        schema=schema,
        point_table_name=point_table_name,
        objectid_field_name=fields.get('object_id_field_name'),
        shape_field_name=fields.get('shape_field_name'),
        srid=srid)
    cursor = connection.cursor()
    # cursor.execute('''truncate table {schema}.{point_table_name}_{srid}'''.format(schema=schema,
    #     point_table_name=point_table_name,
    #     srid=srid))
    cursor.execute(populate_table_stmt)
    connection.commit()

@pytest.fixture
def create_point_view(schema,srid, postgis):
    stmt = ''' 
        CREATE MATERIALIZED VIEW IF NOT EXISTS {schema}.point_table_{srid}_view
        AS
        select * from {schema}.point_table_{srid}
        WITH  DATA '''.format(schema=schema, srid=srid)
    connection = postgis.dbo
    cursor = connection.cursor()
    cursor.execute(stmt)
    connection.commit()

@pytest.fixture
def load_polygon_table(srid, postgis, schema):
    stmt = '''
        DROP TABLE IF EXISTS {schema}.POLYGON_TABLE_{srid};
    CREATE TABLE {schema}.POLYGON_TABLE_{srid}
    (
    {objectid_field_name} numeric,
    {shape_field_name} geometry(Polygon,{srid})
    );

    INSERT INTO {schema}.{polygon_table_name}_{srid} ({shape_field_name}, {objectid_field_name}) 
    VALUES
    (NULL,1),
    (ST_GeomFromText('POLYGON((2697048.19400001 243967.35275000,2697046.11900000 244007.28925000,2697046.19599999 244038.87700000,2696984.16900000 244045.93900000,2697059.92400000 243874.43500000,2697048.19400001 243967.35275000))', {srid}), 2),
    (ST_GeomFromText('POLYGON((2697048.19400001 243967.35275000,2697046.11900000 244007.28925000,2697046.19599999 244038.87700000,2696984.16900000 244045.93900000,2697059.92400000 243874.43500000,2697048.19400001 243967.35275000))', {srid}), 3),
    (ST_GeomFromText('POLYGON((2697048.19400001 243967.35275000,2697046.11900000 244007.28925000,2697046.19599999 244038.87700000,2696984.16900000 244045.93900000,2697059.92400000 243874.43500000,2697048.19400001 243967.35275000))', {srid}), 4),
    (ST_GeomFromText('POLYGON((2697048.19400001 243967.35275000,2697046.11900000 244007.28925000,2697046.19599999 244038.87700000,2696984.16900000 244045.93900000,2697059.92400000 243874.43500000,2697048.19400001 243967.35275000))', {srid}), 5),
    (ST_GeomFromText('POLYGON((2697048.19400001 243967.35275000,2697046.11900000 244007.28925000,2697046.19599999 244038.87700000,2696984.16900000 244045.93900000,2697059.92400000 243874.43500000,2697048.19400001 243967.35275000))', {srid}), 6)
    '''.format(schema=schema,
               polygon_table_name=polygon_table_name,
                srid=srid,
                objectid_field_name = fields.get('object_id_field_name'),
                shape_field_name = fields.get('shape_field_name')
    )
    connection = postgis.dbo
    cursor = connection.cursor()
    # cursor.execute('''truncate table {schema}.{polygon_table_name}_{sr}'''.format(sr=srid,schema=schema,polygon_table_name=polygon_table_name))
    cursor.execute(stmt)
    connection.commit()

@pytest.fixture
def load_line_table(srid, postgis, schema):
    stmt = '''	
        DROP TABLE IF EXISTS {schema}.{line_table_name}_{srid};
    CREATE TABLE {schema}.{line_table_name}_{srid}
    (
    {objectid_field_name} numeric,
    {shape_field_name} geometry(LineString,{srid})
    );
    INSERT INTO {schema}.{line_table_name}_{srid} ({shape_field_name}, {objectid_field_name}) 
    VALUES 
    (NULL, 1),
    (ST_GeomFromText('LINESTRING(2679640.41975001 259205.68799999, 2679610.90800001 259142.53425001)', {srid}), 2),
    (ST_GeomFromText('LINESTRING(2679640.41975001 259205.68799999, 2679610.90800001 259142.53425001)', {srid}), 3),
    (ST_GeomFromText('LINESTRING(2679640.41975001 259205.68799999, 2679610.90800001 259142.53425001)', {srid}), 4),
    (ST_GeomFromText('LINESTRING(2679640.41975001 259205.68799999, 2679610.90800001 259142.53425001)', {srid}), 5),
    (ST_GeomFromText('LINESTRING(2679640.41975001 259205.68799999, 2679610.90800001 259142.53425001)', {srid}), 6)
    '''.format(schema=schema,
               srid=srid,
               line_table_name=line_table_name,
                objectid_field_name=fields.get('object_id_field_name'),
                shape_field_name=fields.get('shape_field_name'))
    connection = postgis.dbo
    cursor = connection.cursor()
    # cursor.execute('''truncate table {schema}.{line_table_name}_{srid}'''.format(schema=schema, srid=srid,line_table_name=line_table_name))
    cursor.execute(stmt)
    connection.commit()

@pytest.fixture
def load_multipolygon_table(srid, postgis, schema):
    stmt = '''
    DROP TABLE IF EXISTS {schema}.{multipolygon_table_name}_{srid};
    CREATE TABLE {schema}.{multipolygon_table_name}_{srid}
    (
    {objectid_field_name} numeric,
    {shape_field_name} geometry(MultiPolygon,{srid})
    );
    
    INSERT INTO {schema}.{multipolygon_table_name}_{srid} ({objectid_field_name}, {shape_field_name}) 
VALUES 
(9, NULL),
(1, ST_GeomFromText('MULTIPOLYGON ((( 2697059.92403972 243874.43507531, 2697057.92404372 243872.43507931, 2697058.92404172 243871.43508130, 2697059.92403972 243872.43507931, 2697059.92403972 243874.43507531)),(( 2697048.19407630 243967.35286848, 2697050.19407231 243968.35286647, 2697049.19407430 243968.35286647, 2697048.19407630 243967.35286848)))', {srid})),
(2, ST_GeomFromText('MULTIPOLYGON ((( 2697059.82397431 243874.43507531, 2697057.92404372 243872.43507931, 2697058.92404172 243871.43508130, 2697059.92403972 243872.43507931, 2697059.82397431 243874.43507531)),(( 2697048.09401089 243967.35286848, 2697050.19407231 243968.35286647, 2697049.19407430 243968.35286647, 2697048.09401089 243967.35286848)))', {srid})),
(3, ST_GeomFromText('MULTIPOLYGON ((( 2697059.98407897 243874.43507531, 2697057.92404372 243872.43507931, 2697058.92404172 243871.43508130, 2697059.92403972 243872.43507931, 2697059.98407897 243874.43507531)),(( 2697048.29414172 243967.35286848, 2697050.19407231 243968.35286647, 2697049.19407430 243968.35286647, 2697048.29414172 243967.35286848)))', {srid})),
(4, ST_GeomFromText('MULTIPOLYGON ((( 2697059.92403972 243874.53514072, 2697057.92404372 243872.43507931, 2697058.92404172 243871.43508130, 2697059.92403972 243872.43507931, 2697059.92403972 243874.53514072)),(( 2697048.19407630 243967.45260581, 2697050.19407231 243968.35286647, 2697049.19407430 243968.35286647, 2697048.19407630 243967.45260581)))', {srid}))
'''.format(schema=schema,
                                    multipolygon_table_name=multipolygon_table_name,
                                    objectid_field_name=fields.get('object_id_field_name'),
                                    shape_field_name=fields.get('shape_field_name'),
                                    srid=srid)
    connection = postgis.dbo
    cursor = connection.cursor()
    #cursor.execute('''truncate table {schema}.{table_name}_{srid}'''.format(schema=schema, srid=srid,
                                                                                 #table_name=multipolygon_table_name))
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
    csv_data = etl.fromcsv(point_csv_dir).cutout('objectid')
    csv_data.topostgis(postgis.dbo, '{}.{}_{}'.format(schema,point_table_name,srid), column_definition_json=point_column_definition, from_srid=srid)


# assert
def assert_data_method(csv_data1, db_data1, srid=None, field=None):
    keys = csv_data1[0]
    i =0

    try:
        db_header = [column[0] for column in db_data1.description]
        db_data1 = db_data1.fetchall()
        i=0
    except:
        db_header = db_data1[0]
        i=1
    
    #for i,row in enumerate(csv_data1[1:]):

    for row in csv_data1[1:]:
        etl_dict = dict(zip(db_header, db_data1[i]))  # dictionary from etl data
        csv_dict = dict(zip(csv_data1[0], row))  # dictionary from csv data
        if field:
            keys = list(field)

        for key in keys:
            print('asserting key ', key)
            csv_val = csv_dict.get(key)
            db_val = etl_dict.get(key)
            print('db_val ', db_val)
            print('csv_val ', csv_val)
            if csv_val == '':
                assert db_val is None
                continue

            # assert shape field
            if key == fields.get('shape_field_name'):
                pg_geom = geom_parser(str(db_val), srid)
                csv_geom = geom_parser(str(csv_val), srid)
                assert csv_geom == pg_geom
            elif key == fields.get('object_id_field_name'):
                continue
            elif key == fields.get('timezone_field_name') or key == fields.get('timestamp_field_name'):
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
                assert db_val == db_val
                
        i=i+1

######################################   TESTS   ####################################################################

#------------------READING TESTS
#load staging data extract using geopetle compare csv data with postgres table data
def test_read_point_table(load_point_table, postgis,csv_data,db_data,srid):
    assert_data_method(csv_data, db_data, srid=srid)

#load staging non geometric data extract using geopetl compare csv data with postgres table data
def test_read_ng_table(load_non_geom_table, postgis,csv_data,schema):
    db_col = etl.frompostgis(dbo=postgis.dbo,table_name='{}.{}_ng'.format(schema,point_table_name))
    csv_data = csv_data.cutout(fields.get('shape_field_name'))
    assert_data_method(csv_data, db_col)

def test_read_without_schema(postgis, csv_data, schema, srid):
    data = etl.frompostgis(dbo=postgis.dbo, table_name='{}.{}_{}'.format(schema,point_table_name,srid))
    assert_data_method(csv_data, data, srid)

#4
def test_read_timestamp(csv_data, db_data,srid):
    key = fields.get('timestamp_field_name')
    assert_data_method(csv_data,db_data, srid, field=key)

#5
def test_read_numericfield(csv_data, db_data,srid):
    key = fields.get('numeric_field_name')
    db_data1 = db_data
    csv_data1 = csv_data
    assert_data_method(csv_data1, db_data1, srid, field=key)
#6
def test_read_datefield(csv_data, db_data, srid):
    key = fields.get('date_field_name')
    db_data1 = db_data
    csv_data1 = csv_data
    assert_data_method(csv_data1, db_data1, srid, field=key)

#7
def test_read_shape(csv_data, db_data,srid):
    key = fields.get('shape_field_name')
    db_data1 = db_data
    csv_data1 = csv_data
    assert_data_method(csv_data1, db_data1, srid, field=key)

# #8
def test_assert_textfield(csv_data, db_data,srid):
    key = fields.get('text_field_name')
    db_data1 = db_data
    csv_data1 = csv_data
    assert_data_method(csv_data1, db_data1, srid, field=key)
# #9
def test_assert_timezone(csv_data, db_data,srid):
    key = fields.get('timezone_field_name')
    db_data1 = db_data
    csv_data1 = csv_data
    assert_data_method(csv_data1, db_data1, srid, field=key)

# #compare csv data with postgres data using geopetl
def test_reading_polygons(postgis, load_polygon_table,schema, srid):
    csv_data = etl.fromcsv(polygon_csv_dir)
    # read data from test DB using petl
    db_data1 = etl.frompostgis(dbo=postgis.dbo, table_name='{}.{}_{}'.format(schema,polygon_table_name,srid))
    assert_data_method(csv_data, db_data1, srid)

def test_reading_linestrings(postgis, load_line_table,schema, srid):
    csv_data = etl.fromcsv(line_csv_dir)
    # read data from test DB using petl
    db_data1 = etl.frompostgis(dbo=postgis.dbo, table_name='{}.{}_{}'.format(schema,line_table_name,srid))
    assert_data_method(csv_data, db_data1, srid)

# #compare csv data with postgres data using geopetl
def test_reading_multipolygons(postgis, load_multipolygon_table,schema, srid):
    csv_data = etl.fromcsv(multipolygon_csv_dir)
    # print('csv_data')
    # print(etl.look(csv_data))
    # read data from test DB using petl
    db_data1 = etl.frompostgis(dbo=postgis.dbo, table_name='{}.{}_{}'.format(schema,multipolygon_table_name,srid))
    # print('db_data1')
    # print(etl.look(db_data1))
    assert_data_method(csv_data, db_data1, srid)

# extract data using sql arg in frompostgis()
def test_stmt_arg(postgis,csv_data,schema,srid):
    qry = '''select {objectid_field_name} ,{text_field_name},{numeric_field_name},
            {timestamp_field_name}, {date_field_name},{timezone_field_name},
             st_astext({shape_field_name}) as {shape_field_name} from {}.{}_{}'''.format(
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
    db_data1 = etl.frompostgis(dbo=postgis.dbo,table_name='{}.{}_{}'.format(schema, point_table_name, srid),sql=qry)
    assert_data_method(csv_data, db_data1, srid)
    
def test_reading_materialized_view(create_point_view, postgis, schema,srid,csv_data):
    db_data1 = etl.frompostgis(dbo=postgis.dbo, table_name='{}.{}_{}_view'.format(schema, point_table_name, srid))
    assert_data_method(csv_data, db_data1, srid)


#------------------WRITING TESTS
def test_write_without_schema(db_data, postgis, csv_data, schema, srid):
    connection = postgis.dbo
    csv_data.topostgis(
            connection,
            '{}_{}'.format(point_table_name, srid),
            from_srid=srid,
            column_definition_json=point_column_definition)
    cursor = connection.cursor()
    stmt = '''
            select {objectid_field_name},{text_field_name},{numeric_field_name},{timestamp_field_name},{date_field_name},
            {timezone_field_name},
            st_astext({shape_field_name}) as {shape_field_name} from {schema}.{table_name}_{srid}'''.format(
        srid=srid,
        schema=schema,
        table_name=point_table_name,
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

def test_write_nongeom_table(postgis, csv_data, schema,srid):
    csv_data = csv_data.cutout('shape')
    print('csv data 412')
    print(etl.look(csv_data))
    csv_data.topostgis(postgis.dbo, '{}.{}_ng'.format(schema,point_table_name)) 
    connection = postgis.dbo
    cursor = connection.cursor()

    stmt = '''
                select {objectid_field_name},{text_field_name},{numeric_field_name},{timestamp_field_name},{date_field_name},
                {timezone_field_name} from {schema}.{point_table_name}_ng'''.format(
        schema=schema,
        point_table_name=point_table_name,
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

# # # WRITING tests write using a string connection to db
def test_write_dsn_connection(csv_data,db, user, pw, host,postgis,schema,srid):
    my_dsn = '''dbname={db} user={user} password={pw} host={host}'''.format(db=db,user=user,pw=pw,host=host)
    etl.topostgis(csv_data,
                  my_dsn,
                  '{}.{}_{}'.format(schema, point_table_name,srid),
                  from_srid=srid,
                  column_definition_json=point_column_definition)
    connection = postgis.dbo
    cursor = connection.cursor()
    stmt = '''
            select {objectid_field_name},{text_field_name},{numeric_field_name},{timestamp_field_name},{date_field_name},
            {timezone_field_name}, st_astext({shape_field_name}) as {shape_field_name} from {schema}.{point_table_name}_{srid}'''.format(
        schema=schema,
        srid=srid,
        point_table_name=point_table_name,
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


# WRITE NULL TIME VALUES
def test_null_times(postgis, csv_data, schema, srid):
    csv_data['timestamp'] = ''
    csv_data['timezone'] = ''
    csv_data['datefield'] = ''
    csv_data.topostgis(postgis.dbo, '{}.{}_{}'.format(schema,point_table_name,srid),
                       column_definition_json=point_column_definition,
                       )
    connection = postgis.dbo
    cursor = connection.cursor()
    stmt = '''
                select {objectid_field_name},{text_field_name},{numeric_field_name},{timestamp_field_name},{date_field_name},
                {timezone_field_name}, booleanfield,
                st_astext({shape_field_name}) as {shape_field_name} from {schema}.{point_table_name}_{srid}'''.format(
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
    assert_data_method(csv_data, cursor, srid)


def test_polygon_assertion_write(postgis, schema, srid):
    csv_data = etl.fromcsv(polygon_csv_dir)
    csv_data.topostgis(postgis.dbo, '{}.{}_{}'.format(schema, polygon_table_name,srid),from_srid=srid,column_definition_json=polygon_column_definition)
    # read data from test DB using petl
    stmt = '''select {objectid_field_name}, ST_AsText({shape_field_name}) as {shape_field_name} from {schema}.{polygon_table_name}_{srid}'''.format(
        schema=schema,
        polygon_table_name=polygon_table_name,
        objectid_field_name = fields.get('object_id_field_name'),
        shape_field_name = fields.get('shape_field_name'),
        srid= srid)
    cursor = postgis.dbo.cursor()
    cursor.execute(stmt)
    assert_data_method(csv_data, cursor, srid)

def test_line_assertion_write(postgis, schema,srid):
    csv_data = etl.fromcsv(line_csv_dir)
    csv_data.topostgis(postgis.dbo, '{}.{}_{}'.format(schema, line_table_name,srid), from_srid=srid,column_definition_json=line_column_definition)
    # read data from test DB
    stmt = '''select {objectid_field_name}, ST_AsText({shape_field_name}) as {shape_field_name} from {schema}.{line_table_name}_{srid}'''.format(
        srid=srid,
        schema=schema,
        line_table_name=line_table_name,
        objectid_field_name = fields.get('object_id_field_name'),
        shape_field_name = fields.get('shape_field_name')
    )
    cursor = postgis.dbo.cursor()
    cursor.execute(stmt)
    assert_data_method(csv_data, cursor, srid)


def test_multipolygon_assertion_write(postgis, schema,srid):
    csv_data = etl.fromcsv(multipolygon_csv_dir)
    csv_data.topostgis(postgis.dbo, '{}.{}_{}'.format(schema, multipolygon_table_name,srid), from_srid=srid)
    # read data from test DB
    stmt = '''select {objectid_field_name}, ST_AsText({shape_field_name}) as {shape_field_name} from {schema}.{table_name}_{srid}'''.format(
        srid=srid,
        schema=schema,
        table_name=multipolygon_table_name,
        objectid_field_name = fields.get('object_id_field_name'),
        shape_field_name = fields.get('shape_field_name')
    )
    cursor = postgis.dbo.cursor()
    cursor.execute(stmt)
    assert_data_method(csv_data, cursor, srid)

#
def test_with_types(db_data, postgis, schema, srid,csv_data):
    create_table_stmt = '''
        DROP TABLE IF EXISTS {schema}.{point_table_name}_{srid}_2;
        CREATE TABLE {schema}.{point_table_name}_{srid}_2
        (
        objectid numeric,
        textfield text,
        "timestamp" timestamp without time zone,
        numericfield numeric,
        timezone timestamp with time zone,
        shape geometry(Point,{srid}),
        datefield date,
        booleanfield boolean
        )'''.format(
        schema=schema,
        point_table_name=point_table_name,
        srid=srid)
    print('createstmt')
    print(create_table_stmt)
    connection = postgis.dbo
    cursor = connection.cursor()
    cursor.execute(create_table_stmt)
    # read data from db
    csv_data.topostgis(postgis.dbo, '{}.{}_{}'.format(schema, point_table_name,srid),
                       column_definition_json=point_column_definition, from_srid=srid)
    data1 = etl.frompostgis(dbo=postgis.dbo,
                            table_name='{}.{}_{}'.format(schema,point_table_name,srid))
    #load to second test table
    data1.topostgis(postgis.dbo, '{}.{}_{}_2'.format(schema, point_table_name,srid), from_srid=srid, column_definition_json=point_column_definition)

    # extract from second test table
    data2 = etl.frompostgis(dbo=postgis.dbo,
                            table_name='{}.{}_{}_2'.format(schema,point_table_name,srid))

    assert_data_method(data1, data2, srid)

