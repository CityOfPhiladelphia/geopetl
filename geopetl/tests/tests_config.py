import re
from dateutil import parser as dt_parser

point_table_name = 'point_table'
point_csv_dir = 'geopetl/tests/fixtures_data/staging/point.csv'
point_column_definition = 'geopetl/tests/fixtures_data/schemas/point.json'

polygon_table_name = 'polygon_table'
polygon_csv_dir = 'geopetl/tests/fixtures_data/staging/polygon.csv'
polygon_column_definition = 'geopetl/tests/fixtures_data/schemas/polygon.json'

line_table_name = 'line_table'
line_csv_dir = 'geopetl/tests/fixtures_data/staging/line.csv'
line_column_definition = 'geopetl/tests/fixtures_data/schemas/line.json'

multipolygon_table_name = 'multipolygon_table'
multipolygon_csv_dir = 'geopetl/tests/fixtures_data/staging/multipolygon.csv'
multipolygon_column_definition = 'geopetl/tests/fixtures_data/schemas/multipolygon.json'

srid_precision = {'2272':3,
                  '4326':8}

fields= {
    'text_field_name': 'textfield',
    'numeric_field_name': 'numericfield',
    'date_field_name': 'datefield',
    'timestamp_field_name': 'timestamp',
    'timezone_field_name': 'timezone',
    'object_id_field_name': 'objectid',
    'shape_field_name': 'shape',
    'boolean_field_name':'booleanfield'
    }

def geom_parser(geom_wkt,srid):
    geom_wkt = str(geom_wkt)
    geom_type = re.findall("[A-Z]{1,12}", geom_wkt)[0]
    coordinates = re.findall(r"[-+]?\d*\.\d+|\d+", geom_wkt)
    coordinates_list = [float("%.{}f".format(srid_precision.get(srid)) % float(coords)) for coords in coordinates]
    return geom_type, coordinates_list


def remove_whitespace(stringval,srid=None):
    shapestring = str(stringval)
    geom_type = re.findall("[A-Z]{1,12}", shapestring)[0]
    coordinates = re.findall(r"[-+]?\d*\.\d+|\d+", shapestring)
    if srid:
        srid = int(srid)
    if srid == 4326:
        #truncate coordinates to 8 decimal places
        coordinates= [str(float('%.8f' % float(coords))) for coords in coordinates]
    elif srid == 2272:
        #truncate coordinates to 3 decimal places
        coordinates= [str(float('%.3f' % float(coords))) for coords in coordinates]
    else:
        #truncate coordinates to 4 decimal places
        coordinates= [str(float('%.6f' % float(coords))) for coords in coordinates]

    if geom_type.lower()  == 'point':
        geom = "{type}({x})".format(type=geom_type, x=" ".join(coordinates))
    elif geom_type.lower()  == 'polygon':
        geom = "{type}(({x}))".format(type=geom_type, x=" ".join(coordinates))
    elif geom_type.lower() == 'linestring':
        geom = "{type}(({x}))".format(type=geom_type, x=" ".join(coordinates))
    return geom



# # assert
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
            csv_val = csv_dict.get(key)
            db_val = etl_dict.get(key)
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
                except Exception as inst:
                    print(type(inst))    # the exception type
                    print(inst.args)     # arguments stored in .args
                    print(inst) 
                    db_val = db_val
                try:
                    csv_val = dt_parser.parse(csv_val)
                except:
                    csv_val = csv_val
                print('type db_val again 186', type(db_val))
                assert db_val == csv_val
            elif key == fields.get('boolean_field_name'):
                if isinstance(csv_val,str):
                    assert csv_val.lower() ==str(db_val).lower()
                else:
                    assert csv_val== db_val
            # compare values from each key
            else:
                assert db_val == db_val
                
        i=i+1
