from collections import OrderedDict
import re
import petl as etl
from petl.compat import string_types
from petl.util.base import Table
from petl.io.db_utils import _quote
from geopetl.util import parse_db_url
import json


DEFAULT_WRITE_BUFFER_SIZE = 1000

DATA_TYPE_MAP = {
    'string':                       'text',
    'number':                       'numeric',
    'float':                        'numeric',
    'double precision':             'numeric',
    'integer':                      'integer',
    'boolean':                      'boolean',
    'object':                       'jsonb',
    'array':                        'jsonb',
    'date':                         'date',
    'time':                         'time',
    'timestamp':                    'timestamp without time zone',
    'timestamp without time zone':  'timestamp without time zone',
    'timestamp with time zone':     'timestamp with time zone',
    'datetime':                     'date',
    'geom':                         'geometry',
    'geometry':                     'geometry'
}

GEOM_TYPE_MAP = {
    'point':           'Point',
    'line':            'Linestring',
    'linestring':      'Linestring',
    'polygon':         'Polygon',
    'multipoint':      'MultiPoint',
    'multipolygon':    'MultiPolygon',
    'multilinestring': 'MultiLineString',
    'geometry':        'Geometry',
}


def frompostgis(dbo, table_name, fields=None, return_geom=True, where=None,
                limit=None):
    """
    Returns an iterable query container.
    Params
    ----------------------------------------------------------------------------
    - dbo:          Can be a DB-API object, SQLAlchemy object, URL string, or
                    connection string.
    - table_name:   Name of the table to read
    - fields:       (optional) A list of fields to select. Defaults to ['*'].
    - return_geom:  (optional) Flag to select and unpack geometry. Set to False
                    to improve performance when geometry is not needed.
                    Defaults to True.
    - where:        (optional) A where clause for the SQL statement.
    - limit:        (optional) Number of rows to return.
    """

    # create db wrappers
    db = PostgisDatabase(dbo)
    table = db.table(table_name)

    # return a query container
    return table.query(fields=fields, return_geom=return_geom, where=where,
                       limit=limit)

etl.frompostgis = frompostgis


def topostgis(rows, dbo, table_name, from_srid=None, column_definition_json=None, buffer_size=DEFAULT_WRITE_BUFFER_SIZE):
    """
    Writes rows to database.
    """
    # create db wrappers
    db = PostgisDatabase(dbo)

    # do we need to create the table?

    table = db.table(table_name)
    create = '.'.join([table.schema, table.name]) not in db.tables
    # sample = 0 if create else None # sample whole table

    if create:
        # TODO create table if it doesn't exist
        print('Autocreate tables for PostGIS not currently implemented!!')
        # request user for json file to create new table
        # column_definition_json = filedialog.askopenfilename(title="Select json file",
        #                                 filetypes=(("json files", "*.json"), ("all files", "*.*")))

        db.create_table(column_definition_json, table)

    if not create:
        table.truncate()
    table.write(rows, from_srid=from_srid)

etl.topostgis = topostgis

def _topostgis(self, dbo, table_name, from_srid=None, column_definition_json=None, buffer_size=DEFAULT_WRITE_BUFFER_SIZE):
    """
    This wraps topostgis and adds a `self` arg so it can be attached to
    the Table class. This enables functional-style chaining.
    """
    return topostgis(self, dbo, table_name, from_srid=from_srid,column_definition_json=column_definition_json, buffer_size=buffer_size)

Table.topostgis = _topostgis


def appendpostgis(rows, dbo, table_name, from_srid=None, buffer_size=DEFAULT_WRITE_BUFFER_SIZE):
    """
    Writes rows to database.
    """
    # create db wrappers
    db = PostgisDatabase(dbo)

    # write
    table = db.table(table_name)
    table.write(rows, from_srid=from_srid)

etl.appendpostgis = appendpostgis

def _appendpostgis(self, dbo, table_name, from_srid=None, buffer_size=DEFAULT_WRITE_BUFFER_SIZE):
    """
    This wraps topostgis and adds a `self` arg so it can be attached to
    the Table class. This enables functional-style chaining.
    """
    return appendpostgis(self, dbo, table_name, from_srid=from_srid, buffer_size=buffer_size)

Table.appendpostgis = _appendpostgis

################################################################################
# DB
################################################################################

class PostgisDatabase(object):
    def __init__(self, dbo):
        import psycopg2
        from psycopg2.extras import RealDictCursor

        # if dbo is a string, create connection object
        if isinstance(dbo, string_types):
            # try to parse as url
            try:
                parsed = parse_db_url(dbo)
                params = {
                   'database':  parsed['db_name'],
                   'user':      parsed['user'],
                   'password':  parsed['password'],
                   'host':      parsed['host'],
                }
                dbo = psycopg2.connect(**params)

            # otherwise assume it's a postgres connection string
            except ValueError:
                dbo = psycopg2.connect(dbo)

        # TODO use petl dbo check/validation

        self.dbo = dbo

        # make a cursor for introspecting the db. not used to read/write data.
        self.cursor = dbo.cursor(cursor_factory=RealDictCursor)

    def __str__(self):
        return 'PostgisDatabase: {}'.format(self.dbo.dsn)

    def __repr__(self):
        return self.__str__()

    def __getitem__(self, key):
        """Alternate notation for getting a table: db['table']"""
        return self.table(key)

    def fetch(self, stmt):
        """Run a SQL statement and fetch all rows."""
        self.cursor.execute(stmt)
        # try:
            # rows = self.cursor.fetchall()
        # lib raises an error if no rows returned
        # except psycopg2.ProgrammingError:
        #     rows = None
        # return rows
        return self.cursor.fetchall()

    # @property
    # def tables(self, schema='public'):
    #     tables = (self.table('information_schema.tables')
    #                   .query(fields=['table_name'],
    #                          where="table_schema = '{}' AND \
    #                                 table_type = 'BASE TABLE'".format(schema))
    #              )
    #     return [x[0] for x in tables]
    @property
    def tables(self):
        tables = (self.table('information_schema.tables')
                      .query(fields=['table_schema', 'table_name'],
                             where="table_type = 'BASE TABLE'")
                 )
        return ['.'.join([x[0], x[1]]) for x in tables]

    def table(self, name):
        return PostgisTable(self, name)


    def create_table(self, schema_dir, table):
        '''
        Creates a table if it doesn't already exist.
        Args: table_name and a json file directory:
            table_name:   string of new table name
            schema_dir:   string of json file direcotry and name
        '''

        fields = self.get_fields_from_jsonfile(schema_dir)

        stmt = '''DROP TABLE IF EXISTS {schema}.{table};
                        CREATE TABLE {schema}.{table}
                        ({fields});'''.format(
            schema = table.schema,
            table = table.name,
            fields = fields)

        self.cursor.execute(stmt)


    def get_fields_from_jsonfile(self, json_schema_dir):  # def get_fields(json_schema_file: str) -> str:
        '''Takes in a json schema file and returns a comma separated string of fields
        and their data types.
        '''
        global logger

        with open(json_schema_dir) as json_file:
            fields = json.load(json_file).get('fields', '')
            if not fields:
                logger.error('Json schema malformatted...')
                raise
            num_fields = len(fields)
            _fields_fmt = ''
            for i, scheme in enumerate(fields):
                scheme_type = DATA_TYPE_MAP.get(scheme['type'].lower(), scheme['type'])
                constraint = scheme.get('constraints', None)
                if scheme_type == 'geometry':
                    scheme_srid = scheme.get('srid', '')
                    scheme_geometry_type = GEOM_TYPE_MAP.get(scheme.get('geometry_type', '').lower(), '')
                    if scheme_srid and scheme_geometry_type:
                        scheme_type = 'geometry({}, {}) '.format(scheme_geometry_type, scheme_srid)
                    else:
                        logger.error('Srid and geometry_type must be provided with geometry field...')
                        raise
                _fields_fmt += ' {} {}'.format(scheme['name'], scheme_type)
                if constraint:
                    _fields_fmt += ' NOT NULL'

                if i < num_fields - 1:
                    _fields_fmt += ','
        return _fields_fmt



################################################################################
# TABLE
################################################################################

# maps db field types to more generic internal ones
FIELD_TYPE_MAP = {
    'integer':                  'num',
    'numeric':                  'num',
    'double precision':         'num',
    'text':                     'text',
    'character varying':        'text',
    'date':                     'date',
    'USER-DEFINED':             'geometry',
    'timestamp with time zone': 'timestamp',
    'timestamp without time zone': 'timestamp',
    'boolean':                  'boolean',
    'uuid':                     'uuid',
    'money':                    'money'
}

class PostgisTable(object):
    def __init__(self, db, name):
        self.db = db

        # Check for a schema
        if '.' in name:
            self.schema, self.name = name.split('.')
        else:
            self.schema = 'public'
            self.name = name

    def __str__(self):
        return 'PostgisTable: {}'.format(self.name)

    def __repr__(self):
        return self.__str__()

    @property
    def metadata(self):
        # stmt = """
        #     select column_name as name, data_type as type
        #     from information_schema.columns
        #     where table_name = '{}'
        # """.format(self.name)
        stmt = """
            select column_name as name, data_type as type
            from information_schema.columns
            where table_schema = '{}' and table_name = '{}'
        """.format(self.schema, self.name)
        fields = self.db.fetch(stmt)
        for field in fields:
            field['type'] = FIELD_TYPE_MAP[field['type']]
        return fields

    @property
    def name_with_schema(self):
        """Returns the table name prepended with the schema name, prepared for
        a query."""

        if self.schema:
            comps = [self.schema, self.name]
            name_with_schema = '.'.join([_quote(x) for x in comps])
        else:
            name_with_schema = self.name
        return name_with_schema

    @property
    def fields(self):
        return [x['name'] for x in self.metadata]

    @property
    def geom_field(self):
        f = [x for x in self.metadata if x['type'] == 'geometry']
        if len(f) == 0:
            return None
        elif len(f) > 1:
            raise LookupError('Multiple geometry fields')
        return f[0]['name']

    def wkt_getter(self, geom_field, to_srid):
        assert geom_field is not None
        geom_getter = geom_field
        if to_srid:
            geom_getter = 'ST_Transform({}, {})'.format(geom_getter, to_srid)
        return 'ST_AsText({}) AS {}'.format(geom_getter, geom_field)

    def get_srid(self):
        stmt = "SELECT Find_SRID('{}', '{}', '{}')"\
                    .format(self.schema, self.name, self.geom_field)
        return self.db.fetch(stmt)[0]['find_srid']

    @property
    def geom_type(self):
        stmt = """
            SELECT type
            FROM geometry_columns
            WHERE f_table_schema = '{}'
            AND f_table_name = '{}'
            and f_geometry_column = '{}';
        """.format(self.schema, self.name, self.geom_field)
        return self.db.fetch(stmt)[0]['type']

    @property
    def non_geom_fields(self):
        return [x for x in self.fields if x != self.geom_field]

    def query(self, fields=None, return_geom=None, where=None, limit=None):
        return PostgisQuery(self.db, self, fields=fields,
                           return_geom=return_geom, where=where, limit=limit)

    def prepare_val(self, val, type_):
        """Prepare a value for entry into the DB."""
        if type_ == 'text':
            if val:
                val = str(val)
                # escape single quotes
                val = val.replace("'", "''")
            else:
                val = ''
            val = "'{}'".format(val)
        elif type_ == 'num':
            if val:
                val = str(val)
            else:
                val = 'NULL'
        elif type_ == 'date':
            # TODO dates should be converted to real dates, not strings
            if val:
                val = str(val)
                val = "'{}'".format(val)
            else:
                val = 'NULL'
        elif type_ == 'geometry':
            val = str(val)
        elif type_ == 'timestamp':
            if not val:
                val = 'NULL'
            elif 'timestamp' not in val.lower():
                val = '''TIMESTAMP '{}' '''.format(val)
            else:
                val = val
        elif type_ == 'boolean':
            val = val
        else:
            raise TypeError("Unhandled type: '{}'".format(type_))
        return val

    def _prepare_geom(self, geom, srid, transform_srid=None, multi_geom=True):
        """Prepares WKT geometry by projecting and casting as necessary."""
        geom = "ST_GeomFromText('{}', {})".format(geom, srid) if geom else "null"

        # Handle 3D geometries
        # TODO: screen these with regex
        if 'NaN' in geom:
            geom = geom.replace('NaN', '0')
            geom = "ST_Force_2D({})".format(geom)

        # Convert curve geometries (these aren't supported by PostGIS)
        if 'CURVE' in geom or geom.startswith('CIRC'):
            geom = "ST_CurveToLine({})".format(geom)
        # Reproject if necessary
        if transform_srid and srid != transform_srid:
             geom = "ST_Transform({}, {})".format(geom, transform_srid)
        # else:
        #   geom = "ST_GeomFromText('{}', {})".format(geom, from_srid)

        if multi_geom:
            geom = 'ST_Multi({})'.format(geom)

        return geom

    def write(self, rows, from_srid=None, buffer_size=DEFAULT_WRITE_BUFFER_SIZE):
        """
        Inserts dictionary row objects in the the database
        Args: list of row dicts, table name, ordered field names

        This doesn't currently use petl.todb for a few reasons:
            - petl uses executemany which isn't intended for speed (basically
              the equivalent of running many insert statements)
            - calls to DB functions like ST_GeomFromText end up getting quoted;
              not sure how to disable this.
        """

        # Get fields from the row because some fields from self.fields may be
        # optional, such as autoincrementing integers.
        # raise
        #fields = rows.header()
        fields = rows[0]
        geom_field = self.geom_field

        # convert rows to records (hybrid objects that can behave like dicts)
        rows = etl.records(rows)

        # Get geom metadata
        if geom_field:
            srid = from_srid or self.get_srid()
            #row_geom_type = re.match('[A-Z]+', rows[0][geom_field]).group() \
            #    if geom_field and rows[0][geom_field] else None
            match = re.match('[A-Z]+', rows[0][geom_field])
            row_geom_type = match.group() if match else None
            table_geom_type = self.geom_type if geom_field else None

        # Do we need to cast the geometry to a MULTI type? (Assuming all rows
        # have the same geom type.)
        if geom_field:
            if self.geom_type.startswith('MULTI') and \
                not row_geom_type.startswith('MULTI'):
                multi_geom = True
            else:
                multi_geom = False

        # Make a map of non geom field name => type
        type_map = OrderedDict()
        for field in fields:
            try:
                type_map[field] = [x['type'] for x in self.metadata if x['name'] == field][0]
            except IndexError:
                raise ValueError('Field `{}` does not exist'.format(field))
        type_map_items = type_map.items()

        fields_joined = ', '.join(fields)
        stmt = "INSERT INTO {} ({}) VALUES ".format('.'.join([self.schema, self.name]), fields_joined)

        len_rows = len(rows)
        if buffer_size is None or len_rows < buffer_size:
            iterations = 1
        else:
            iterations = int(len_rows / buffer_size)
            iterations += (len_rows % buffer_size > 0)  # round up

        execute = self.db.cursor.execute
        commit = self.db.dbo.commit

        # Make list of value lists
        val_rows = []
        cur_stmt = stmt

        # DEBUG
        import psycopg2

        for i, row in enumerate(rows):
            val_row = []
            for field, type_ in type_map_items:
                if type_ == 'geometry':
                    geom = row[geom_field]
                    val = self._prepare_geom(geom, srid, multi_geom=multi_geom)
                    val_row.append(val)

                else:
                    val = self.prepare_val(row[field], type_)
                    val_row.append(val)
            val_rows.append(val_row)

            # check if it's time to ship a chunk
            if i % buffer_size == 0:
                # Execute
                vals_joined = ['({})'.format(', '.join(vals)) for vals in val_rows]
                rows_joined = ', '.join(vals_joined)
                cur_stmt += rows_joined
                try:
                    execute(cur_stmt)
                except psycopg2.ProgrammingError:
                    print(self.db.cursor.query)
                    raise
                commit()

                val_rows = []
                cur_stmt = stmt

        # Execute remaining rows (TODO clean this up)
        vals_joined = ['({})'.format(', '.join(vals)) for vals in val_rows]
        rows_joined = ', '.join(vals_joined)
        cur_stmt += rows_joined
        execute(cur_stmt)
        commit()

    # def truncate(self, cascade=False):
    #     """Drop all rows."""
    #
    #     name = self.name
    #     # RESTART IDENTITY resets sequence generators.
    #     stmt = "TRUNCATE {} RESTART IDENTITY".format(name)
    #     if cascade:
    #         stmt += ' CASCADE'
    #
    #     self.db.cursor.execute(stmt)
    #     self.db.dbo.commit()
    def truncate(self, cascade=False):
        """Drop all rows."""

        name = self.name
        schema = self.schema
        # RESTART IDENTITY resets sequence generators.
        stmt = "TRUNCATE {} RESTART IDENTITY".format('.'.join([schema, name]))
        if cascade:
            stmt += ' CASCADE'

        self.db.cursor.execute(stmt)
        self.db.dbo.commit()

################################################################################
# QUERY
################################################################################

class PostgisQuery(Table):
    def __init__(self, db, table, fields=None, return_geom=True, to_srid=None,
                 where=None, limit=None):
        self.db = db
        self.table = table
        self.fields = fields
        self.return_geom = return_geom
        self.to_srid = to_srid
        self.where = where
        self.limit = limit

    def __iter__(self):
        """Proxy iteration to core petl."""
        # form sql statement
        stmt = self.stmt()

        # get petl iterator
        dbo = self.db.dbo
        db_view = etl.fromdb(dbo, stmt)
        iter_fn = db_view.__iter__()

        return iter_fn

    def stmt(self):
        # handle fields
        fields = self.fields
        if fields is None:
            # default to non geom fields
            fields = self.table.non_geom_fields
        fields = [_quote(field) for field in fields]

        # handle geom
        geom_field = self.table.geom_field
        if geom_field and self.return_geom:
            wkt_getter = self.table.wkt_getter(geom_field, self.to_srid)
            fields.append(wkt_getter)

        # form statement
        fields_joined = ', '.join(fields)
        stmt = 'SELECT {} FROM {}'.format(fields_joined,
                                          self.table.name_with_schema)

        where = self.where
        if where:
            stmt += ' WHERE {}'.format(where)

        limit = self.limit
        if limit:
            stmt += ' LIMIT {}'.format(limit)

        return stmt

