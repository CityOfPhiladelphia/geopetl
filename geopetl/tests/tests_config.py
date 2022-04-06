import re
import re
point_table_name = 'point_table'
point_csv_dir = 'geopetl/tests/fixtures_data/staging/point.csv'

polygon_table_name = 'polygon_table'
polygon_csv_dir = 'geopetl/tests/fixtures_data/staging/polygon.csv'

line_table_name = 'line_table'
line_csv_dir = 'geopetl/tests/fixtures_data/staging/line.csv'


point_column_definition = 'geopetl/tests/fixtures_data/schemas/point.json'
polygon_column_definition = 'geopetl/tests/fixtures_data/schemas/polygon.json'
line_column_definition = 'geopetl/tests/fixtures_data/schemas/line.json'

srid_precision = {'2272':3,
                  '4326':8}

fields= {
    'text_field_name': 'textfield',
    'numeric_field_name': 'numericfield',
    'date_field_name': 'datefield',
    'timestamp_field_name': 'timestamp',
    'timezone_field_name': 'timezone',
    'object_id_field_name': 'objectid',
    'shape_field_name': 'shape'
          }

def geom_parser(geom_wkt,srid):
    geom_wkt = str(geom_wkt)
    print('parsing geom wkt ', geom_wkt)
    geom_type = re.findall("[A-Z]{1,12}", geom_wkt)[0]
    print('geom_type ',geom_type)

    coordinates = re.findall(r"[-+]?\d*\.\d+|\d+", geom_wkt)
    print('coordinates ',coordinates )
    print('srid ',srid )
    print(type(srid) )
    print('precision ', srid_precision.get(srid))
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