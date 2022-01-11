import pytest
import petl as etl
from geopetl.postgis import PostgisDatabase
import psycopg2
from pytz import timezone
import csv
from dateutil import parser as dt_parser
from tests_config import remove_whitespace, line_csv_dir, line_table_name, polygon_csv_dir, line_table_name,polygon_table_name,point_table_name, point_csv_dir


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
def create_test_tables(postgis, column_definition,schema):
    # populate a new geopetl table object with staging data from csv file
    rows = etl.fromcsv(point_csv_dir)
    # write geopetl table to postpostgis
    rows.topostgis(postgis.dbo, '{}.{}'.format(schema,point_table_name), column_definition_json=column_definition, from_srid=2272)

@pytest.fixture
def csv_data():
    csv_data = etl.fromcsv(point_csv_dir).convert(['objectid','numericfield'], int)
    csv_data = etl.convert(csv_data,['timestamp','datefield','timezone'], lambda row: dt_parser.parse(row))
    csv_data = etl.convert(csv_data, 'datefield', lambda row: row.date())
    return csv_data


@pytest.fixture
def db_data(postgis,schema):
    db_col = etl.frompostgis(dbo=postgis.dbo,table_name='{}.{}'.format(schema,point_table_name))
    return db_col

@pytest.fixture
def create_test_table_noid(postgis, schema,column_definition):
    csv_data = etl.fromcsv(point_csv_dir).cutout('objectid')
    csv_data.topostgis(postgis.dbo, '{}.{}'.format(schema,point_table_name), column_definition_json=column_definition, from_srid=2272)


######################################   TESTS   ####################################################################

# compare number of rows
def test_all_rows_written(db, user, host, pw,create_test_tables,schema):
    # read staging data from csv
    with open(point_csv_dir, newline='') as f:
        reader = csv.reader(f)
        csv_data = list(reader)

    # connect to postgres DB using psycopg2
    connection = psycopg2.connect(user=user,
                                  host=host,
                                  password=pw,
                                  database=db)
    cur = connection.cursor()
    # query all data from postgres table
    cur.execute('Select * from {table}'.format(table= '{}.{}'.format(schema,point_table_name)))
    result = cur.fetchall()

    # assert number of rows written to pg with test data
    assert len(csv_data[1:]) == len(result)


# compare csv data with postgres data using psycopg2
def test_assert_data(postgis, csv_data, schema):
    csv_data = csv_data
    csv_header = csv_data[0]

    # read data using postgres
    cur = postgis.dbo.cursor()
    cur.execute('select objectid,textfield,timestamp,numericfield, timezone, st_astext(shape) as shape, datefield from ' + '{}.{}'.format(schema,point_table_name))
    db_data = cur.fetchall()
    db_header = [column[0] for column in cur.description]

    i=1
    # iterate through each row of data
    for row in db_data:
        # create dictionary for each row of data
        csv_dict = dict(zip(csv_header, csv_data[i]))       # dictionary from csv data
        pg_dict = dict(zip(db_header, row))                 # dictionary from postgres data
        # iterate through each keys
        for key in db_header:
            # compare values from each key
            if key=='shape':
                pg_geom = remove_whitespace(str(pg_dict.get(key)))
                csv_geom = remove_whitespace(str(csv_dict.get(key)))
                assert csv_geom == pg_geom
            elif key =='objectid':
                continue
            else:
                assert csv_dict.get(key) == pg_dict.get(key)

        i=i+1


#compare csv data with postgres data using geopetl
def test_assert_data_2(postgis, schema,csv_data,db_data):
    csv_header = csv_data[0]

    # read data using geopetl
    # db_data = etl.frompostgis(dbo=postgis.dbo, table_name='{}.{}'.format(schema, point_table_name),fields=['textfield','datefield','timestamp','numericfield','shape','timezone'])
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
            if key=='shape':
                pg_geom = remove_whitespace(str(etl_dict.get(key)))
                csv_geom = remove_whitespace(str(csv_dict.get(key)))
                assert csv_geom == pg_geom
            elif key =='objectid':
                continue
            # compare values from each key
            else:
                assert csv_dict.get(key) == etl_dict.get(key)
        i = i+1

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


# # assert DB data with itself
def test_with_types( db_data, postgis, column_definition, schema):#create_test_tables
    # read data from db
    data1 = db_data
    #load to second test table
    etl.topostgis(db_data, postgis.dbo, '{}.{}2'.format(schema,point_table_name), column_definition_json=column_definition,  from_srid=2272)
    #extract from second test table
    data2 = etl.frompostgis(dbo=postgis.dbo,table_name='{}.{}2')
    i = 1
    # iterate through each row of DB data
    for row in db_data[1:]:
        # create dictionary with header and each row of data
        db_dict1 = dict(zip(data1[0], data1[i]))
        db_dict2 = dict(zip(data2[0], data2[i]))
        # iterate through each keys
        for key in db_data[0]:
            # assert shape field
            if key == 'objectid':
                continue
            if key == 'shape':
                geom1 = remove_whitespace(str(db_dict1.get(key)))
                geom2 = remove_whitespace(str(db_dict2.get(key)))
                assert geom1 == geom2
            # assert values from each key
            else:
                assert db_dict1.get(key) == db_dict2.get(key)
        i = i + 1

# # assert data by loading and extracting data without providing schema
def test_without_schema(db_data, postgis, column_definition, csv_data, schema,create_test_tables):
    data = etl.frompostgis(dbo=postgis.dbo, table_name='{}.{}'.format(schema,point_table_name))

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
                elif key == 'shape':
                    pg_geom = remove_whitespace(str(etl_dict.get(key)))
                    csv_geom = remove_whitespace(str(csv_dict.get(key)))
                    assert csv_geom == pg_geom
                # compare values from each key
                else:
                    assert csv_dict.get(key) == etl_dict.get(key)
            i = i + 1
#
# # write using a string connection to db
def test_dsn_connection(csv_data,db, user, pw, host,postgis, column_definition,schema,create_test_tables):
    mydsn = '''dbname={db} user={user} password={pw} host={host}'''.format(db=db,user=user,pw=pw,host=host)
    my_dsn = '''dbname={db} user={user} password={pw} host={host}'''.format(db=db,user=user,pw=pw,host=host)
    etl.topostgis(csv_data, my_dsn,
                  '{}.{}'.format(schema,point_table_name),
                  from_srid=2272, column_definition_json=column_definition)
    data = etl.frompostgis(dbo=postgis.dbo, table_name='{}.{}'.format(schema,point_table_name))
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
                if key =='objectid':
                    continue
                    # assert csv_dict.get(key) == etl_dict.get(key)
                # assert shape field
                elif key == 'shape':
                    pg_geom = remove_whitespace(str(etl_dict.get(key)))
                    csv_geom = remove_whitespace(str(csv_dict.get(key)))
                    assert csv_geom == pg_geom
                # compare values from each key
                else:
                    assert csv_dict.get(key) == etl_dict.get(key)
            i = i + 1


# # load csv data to postgressde db without an objectid field using geopetl and assert data
def test_assert_data_no_id(create_test_table_noid,csv_data, db_data):
    # list of column names
    i=1
    # iterate through each row of data
    for row in db_data[1:]:
        # create dictionary for each row of data using same set of keys
        csv_dict = dict(zip(csv_data[0], csv_data[i]))         # dictionary from csv data
        oracle_dict = dict(zip(db_data[0], row))        # dictionary from postgres data

        for key in csv_data[0]:
            if key == 'objectid' and 'objectid' in csv_data[0] and 'objectid' in db_data[0]:
                assert (oracle_dict.get('objectid') is not None)
            elif key == 'shape':
                pg_geom = remove_whitespace(str(oracle_dict.get('shape')))
                csv_geom = remove_whitespace(str(csv_dict.get('shape')))
                assert csv_geom == pg_geom
            else:
                assert oracle_dict.get(key) == csv_dict.get(key)
        i=i+1


# # # assert DB data with itself
def test_with_types(create_test_tables, db_data,schema, postgis, column_definition):
    # read data from db
    data1 = db_data
    #load to second test table
    etl.topostgis(db_data, postgis.dbo, '{}.{}2'.format(schema,point_table_name), column_definition_json=column_definition,  from_srid=2272)
    #extract from second test table
    data2 = etl.frompostgis(dbo=postgis.dbo,table_name='{}.{}2'.format(schema,point_table_name))
    i = 1
    # iterate through each row of DB data
    for row in db_data[1:]:
        # create dictionary with header and each row of data
        db_dict1 = dict(zip(data1[0], data1[i]))
        db_dict2 = dict(zip(data2[0], data2[i]))
        # iterate through each keys
        for key in db_data[0]:
            # assert shape field
            if key == 'objectid':
                continue
            if key == 'shape':
                geom1 = remove_whitespace(str(db_dict1.get(key)))
                geom2 = remove_whitespace(str(db_dict2.get(key)))
                assert geom1 == geom2
            # assert values from each key
            else:
                assert db_dict1.get(key) == db_dict2.get(key)
        i = i + 1
# #
# # # assert data by loading and extracting data without providing schema
def test_without_schema(db_data, postgis, column_definition, csv_data,create_test_tables):
    #etl.topostgis(csv_data, postgis.dbo, table_name_no_schema)#, from_srid=2272)
                  #column_definition_json=column_definition)
    data = etl.frompostgis(dbo=postgis.dbo, table_name=point_table_name)
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
                elif key == 'shape':
                    pg_geom = remove_whitespace(str(etl_dict.get(key)))
                    csv_geom = remove_whitespace(str(csv_dict.get(key)))
                    assert csv_geom == pg_geom
                # compare values from each key
                else:
                    assert csv_dict.get(key) == etl_dict.get(key)
            i = i + 1

# # # write using a string connection to db
def test_dsn_connection(csv_data,db, user, pw, host,postgis, column_definition,schema):
    my_dsn = '''dbname={db} user={user} password={pw} host={host}'''.format(db=db,user=user,pw=pw,host=host)
    tb = postgis.table('{}.{}'.format(schema,point_table_name))
    etl.topostgis(csv_data, my_dsn,
                  point_table_name,
                  from_srid=2272, column_definition_json=column_definition)
    data = etl.frompostgis(dbo=postgis.dbo, table_name='{}.{}'.format(schema,point_table_name))
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
                if key =='objectid':
                    continue
                # assert shape field
                if key == tb.geom_field:
                    pg_geom = remove_whitespace(str(etl_dict.get(key)))
                    csv_geom = remove_whitespace(str(csv_dict.get(key)))
                    assert csv_geom == pg_geom
                # compare values from each key
                else:
                    assert csv_dict.get(key) == etl_dict.get(key)
            i = i + 1
#
#
# #compare csv data with postgres data using geopetl
def test_line_assertion(postgis, csv_data,schema, column_definition):
    tb = postgis.table('{}.{}'.format(schema,line_table_name))
    rows = etl.fromcsv(line_csv_dir)
    rows.topostgis(postgis.dbo, '{}.{}'.format(schema,line_table_name))
    csv_data = etl.fromcsv(line_csv_dir).convert(['objectid'], int)
    # list of column names
    keys = csv_data[0]

    # read data from test DB using petl
    db_data = etl.frompostgis(dbo=postgis.dbo, table_name='test_user.line_table')

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
            elif key == 'objectid':
                continue
            # compare values from each key
            else:
                assert csv_dict.get(key) == etl_dict.get(key)
        i = i+1


#compare csv data with postgres data using geopetl
def test_polygon_assertion(postgis, schema, csv_data, column_definition):
    tb = postgis.table('{}.{}'.format(schema,polygon_table_name))
    rows = etl.fromcsv(polygon_csv_dir)
    rows.topostgis(postgis.dbo, '{}.{}'.format(schema,polygon_table_name))
    csv_data = etl.fromcsv(polygon_csv_dir).convert(['objectid'], int)
    # list of column names
    keys = csv_data[0]

    # read data from test DB using petl
    db_data = etl.frompostgis(dbo=postgis.dbo, table_name='{}.{}'.format(schema,polygon_table_name))

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
            elif key =='objectid':
                continue
            # compare values from each key
            else:
                assert csv_dict.get(key) == etl_dict.get(key)
        i = i+1