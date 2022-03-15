import re
import re
point_table_name = ''
point_csv_dir = 'geopetl/tests/fixtures_data/staging/point.csv'

polygon_table_name = ''
polygon_csv_dir = 'geopetl/tests/fixtures_data/staging/polygon.csv'

line_table_name = ''
line_csv_dir = 'geopetl/tests/fixtures_data/staging/line.csv'


point_column_definition = 'geopetl/tests/fixtures_data/schemas/point.json'
polygon_column_definition = 'geopetl/tests/fixtures_data/schemas/polygon.json'
line_column_definition = 'geopetl/tests/fixtures_data/schemas/line.json'

srid_precision = {'2272':1,
                  '4326':1}

fields= {
    'text_field_name': '',
    'numeric_field_name': '',
    'date_field_name': '',
    'timestamp_field_name': '',
    'timezone_field_name': '',
    'object_id_field_name': '',
    'shape_field_name': ''
          }

def geom_parser(geom_wkt,srid):
    geom_wkt = str(geom_wkt)
    geom_type = re.findall("[A-Z]{1,12}", geom_wkt)[0]
    coordinates = re.findall(r"[-+]?\d*\.\d+|\d+", geom_wkt)
    coordinates_list = [float("%.{}f".format(srid_precision.get(srid)) % float(coords)) for coords in coordinates]
    return geom_type, coordinates_list
