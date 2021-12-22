import re
point_table_name = ''
point_csv_dir = ''
polygon_table_name = ''
polygon_csv_dir = ''
line_table_name = ''
line_csv_dir = ''



def remove_whitespace(stringval):
    shapestring = str(stringval)
    geom_type = re.findall("[A-Z]{1,12}", shapestring)[0]
    coordinates = re.findall(r"[-+]?\d*\.\d+|\d+", shapestring)
    coordinates= [str(round(float(coords),4))for coords in coordinates]
    if geom_type == 'point' or geom_type=="POINT":
        geom = "{type}({x})".format(type=geom_type, x=" ".join(coordinates))
    elif geom_type == 'polygon' or geom_type=="POLYGON":
        geom = "{type}(({x}))".format(type=geom_type, x=" ".join(coordinates))
    elif geom_type == 'linestring' or geom_type=="LINESTRING":
        geom = "{type}(({x}))".format(type=geom_type, x=" ".join(coordinates))
    return geom