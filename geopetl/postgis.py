from collections import OrderedDict
import re
import petl as etl
from petl.compat import string_types
from petl.util.base import Table
from petl.io.db_utils import _quote
from geopetl.util import parse_db_url
import json
from dateutil import parser as dt_parser
# For some errors below
import psycopg2


DEFAULT_WRITE_BUFFER_SIZE = 1000

DATA_TYPE_MAP = {
    'smallint':                     'numeric',
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


def frompostgis(dbo, table_name, fields=None, return_geom=True, geom_with_srid=False,
                where=None, limit=None, sql=None):
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
    return table.query(fields=fields, return_geom=return_geom, geom_with_srid=geom_with_srid,
                       where=where, limit=limit, sql=sql)

etl.frompostgis = frompostgis


def topostgis(rows, dbo, table_name, from_srid=None, column_definition_json=None, buffer_size=DEFAULT_WRITE_BUFFER_SIZE):
    """
    Writes rows to database.
    """
    # create db wrappers
    db = PostgisDatabase(dbo)
    # do we need to create the table?
    table = db.table(table_name)
    # sample = 0 if create else None # sample whole table
    create = '.'.join([table.schema, table.name]) not in db.tables
    # Create table if it doesn't exist
    if create:
        # Disable autocreate new postgres table
        if db.is_sde_enabled:
            print('Autocreate tables for Postgres SDE not currently implemented!!')
            raise
        # create new postgis table
        else:
            print('Autocreating PostGIS table')
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

        # elif isinstance(dbo,psycopg2.extensions.connection):
        #     pass
        # REVIEW petl already handles db api connections?
        # # is it a callable?
        # elif callable(dbo):
        #     dbo = dbo()

        # elif callable(dbo):
        #     dbo = dbo()

        # make a cursor for introspecting the db. not used to read/write data.
        self.cursor = dbo.cursor(cursor_factory=RealDictCursor)

        a = dbo.get_dsn_parameters()

        # TODO use petl dbo check/validation
        self.dbo = dbo
        self.user = a.get('user')

        # To be used by setter properties below
        self._is_sde_enabled = None
        self._is_postgis_enabled = None


    def __str__(self):
        return 'PostgisDatabase: {}'.format(self.dbo.dsn)

    def __repr__(self):
        return self.__str__()

    def __getitem__(self, key):
        """Alternate notation for getting a table: db['table']"""
        return self.table(key)

    def fetch(self, stmt):
        """Run a SQL statement and fetch all rows."""
        try:
            self.cursor.execute(stmt)
            return self.cursor.fetchall()
        except Exception as e:
            self.cursor.execute("ROLLBACK")
            raise e

    @property
    def is_sde_enabled(self):
        # Get the value
        if self._is_sde_enabled is not None:
            return self._is_sde_enabled
        # Set the value only once (saves on db calls)
        elif self._is_sde_enabled is None:
            sde_exists = "SELECT to_regclass('sde.sde_version') as exists"
            self.cursor.execute(sde_exists)
            result = self.cursor.fetchall()
            # Result will always be in an 'exists' value of either None or the name of the table.
            result = result[0]['exists']
            if result:
                self._is_sde_enabled = True
            if not result:
                self._is_sde_enabled = False
            return self._is_sde_enabled

    @property
    def is_postgis_enabled(self):
        # Get the value
        if self._is_postgis_enabled is not None:
            return self._is_postgis_enabled
        # Set the value only once (saves on db calls)
        if self._is_postgis_enabled is None:
            self.cursor.execute("SELECT proname FROM pg_proc WHERE proname LIKE 'postgis_version';")
            result = self.cursor.fetchall()
            # This will return an empty string if it doesn't exist, so test like this instead.
            if result:
                self._is_postgis_enabled = True
            else:
                self._is_postgis_enabled = False
            return self._is_postgis_enabled



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
        stmt = "select table_schema || '.' || table_name as table from information_schema.tables where table_type = 'BASE TABLE'"
        cur = self.dbo.cursor()
        cur.execute(stmt)
        tables = [t[0] for t in cur.fetchall()]
        return tables
#        tables = (self.table('information_schema.tables')
#                      .query(fields=['table_schema', 'table_name'],
#                             where="table_type = 'BASE TABLE'")
#                 )
#        return ['.'.join([x[0], x[1]]) for x in tables]
    @property
    def get_schemas(self):
        cur = self.dbo.cursor()
        cur.execute('select distinct schema_name FROM information_schema.schemata')
        schemas = [t[0] for t in cur.fetchall()]
        return schemas

    def table(self, name):
        return PostgisTable(self, name)


    def create_table(self, schema_dir, table):
        '''
        Creates a table if it doesn't already exist.
        Args: table_name and a json file directory:
            table_name:   string of new table name
            schema_dir:   string of json file direcotry and name
        '''

        # if schema dir = None
        if not schema_dir:
            # raise error
            print("Create new Postgis table feature requires column definition json file.")
            raise
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
                    if self.is_sde_enabled:
                        scheme_type= 'st_geometry'
                    else:
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
    'smallint':                 'num',
    'int8':                     'num',
    'int4':                     'num',
    'int2':                     'num',
    'integer':                  'num',
    'smallint':                 'num',
    'float':                    'num',
    'double':                   'num',
    'numeric':                  'num',
    'bigint':                   'num',
    'double precision':         'num',
    'text':                     'text',
    'character varying':        'text',
    'varchar':                  'text',
    'date':                     'date',
    'user-defined':             'geometry',
    'st_geometry':              'geometry',
    'geometry':                 'geometry',     # postgis materialized view returns type geometry instead of user-defined or st_geometry
    'st_point':                 'geometry',     # sde mat view?
    'timestamp with time zone': 'timestamptz',
    'timestamptz':              'timestamptz',  # postgis materialized view returns type timestamptz instead of timestamp with time zone
    'timestamp without time zone': 'timestamp',
    # materialized views don't specify if they use a timezone, this could be problematic.
    'timestamp':                'timestamp',
    'boolean':                  'boolean',
    'bool':                     'boolean',
    'uuid':                     'uuid',
    'money':                    'money',
    'bytea':                    'text',
    'array':                    'text'
}

class PostgisTable(object):

    _geom_field = None
    _srid = None
    _database_object_type = None

    def __init__(self, db, name):
        self.db = db

        # Check for a schema
        if '.' in name:
            self.schema, self.name = name.split('.')
        #schema not provided
        elif self.db.user not in self.db.get_schemas:
            self.schema = 'public'
            self.name = name
        else:
            self.schema = self.db.user
            self.name = name
        self._is_sde_registered = None


    def __str__(self):
        return 'PostgisTable: {}'.format(self.name)

    def __repr__(self):
        return self.__str__()

    @property
    def database_object_type(self):
        """returns whether the object is a table, view, or materialized view using pg_class
        to figure out the type of object we're interacting with.
        docs: https://www.postgresql.org/docs/11/catalog-pg-class.html
        Right now we want to know whether its a table, view, or materialized view. Other
        things shouldn't be getting passed and we'll raise an exception if they are.
        """
        if self._database_object_type:
            return self._database_object_type
        type_map = {
            'r': 'table','i': 'index','S': 'sequence','t': 'TOAST_table','v': 'view', 'm': 'materialized_view',
            'c': 'composite_type','f': 'foreign_table','p': 'partitioned_table','I': 'partitioned_index'
        }
        stmt = """
            SELECT relkind FROM pg_class
            JOIN pg_catalog.pg_namespace n ON n.oid = pg_class.relnamespace
            WHERE relname='{}'
            AND n.nspname='{}';
            """.format(self.name,self.schema)
        if self.name:
            res = self.db.fetch(stmt)
            relkind = res[0]['relkind']
        else:
            # table arg in frompostgis method can defaut to None when custom sql is provided via sql kwarg
            return 'sql'
        if type_map[relkind] in ['table', 'materialized_view', 'view']:
            self._database_object_type = type_map[relkind]
            print('Database object type: {}.'.format(self._database_object_type))
            return self._database_object_type
        else:
            raise TypeError("""This database object is unsupported at this time.
            database object passed to us looks like a '{}'""".format(type_map[relkind]))


    @property
    def is_sde_registered(self):
        if self._is_sde_registered is not None:
            return self._is_sde_registered
        # Check if it's registered by checking it's objectid column (should be objectid)
                    # in the SDE table registry
        if not self.db.is_sde_enabled:
            self._is_sde_registered = False
        else:
            stmt = '''select rowid_column from sde.sde_table_registry where
                                    table_name = '{table_name}' and schema = '{table_schema}' '''.format(table_name=self.name, table_schema=self.schema)
            sde_register_check = self.db.fetch(stmt)
            print("sde register check: ", sde_register_check)
            if sde_register_check:
                self._is_sde_registered = True
            else:
                self._is_sde_registered = False
        return self._is_sde_registered


    @property
    def metadata(self):
        if self.database_object_type == 'table':
            stmt = """
                select column_name as name, data_type as type
                from information_schema.columns
                where table_schema = '{}' and table_name = '{}'
                """.format(self.schema, self.name)
        # Funny method for getting the column names and data types for views
        elif self.database_object_type == 'materialized_view' or self.database_object_type == 'view':
            stmt = """
                select 
                    attr.attname as name,
                    trim(leading '_' from tp.typname) as type
                from pg_catalog.pg_attribute as attr
                join pg_catalog.pg_class as cls on cls.oid = attr.attrelid
                join pg_catalog.pg_namespace as ns on ns.oid = cls.relnamespace
                join pg_catalog.pg_type as tp on tp.typelem = attr.atttypid
                where 
                    ns.nspname = '{}' and
                    cls.relname = '{}' and 
                    not attr.attisdropped and 
                    cast(tp.typanalyze as text) = 'array_typanalyze' and 
                    attr.attnum > 0
                order by 
                    attr.attnum
                """.format(self.schema,self.name)
        fields = self.db.fetch(stmt)
        # RealDictRows don't accept normal key removals like .pop for whatever reason
        # only removal by index number works.
        # gdb_geomattr_data is a postgis specific column added automatically by arc programs
        # we don't need to worry about this field so we should remove it.
        # docs: https://support.esri.com/en/technical-article/000001196
        for i, field in enumerate(fields):
            if field['name'] == 'gdb_geomattr_data':
                del fields[i]

        for field in fields:
            field['type'] = FIELD_TYPE_MAP[field['type'].lower()]
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
        if self._geom_field is None:
            # Assume non-geometric, if we find a geometry this var will be set below
            target_table_shape_fields = None
            if self.db.is_sde_enabled:
                if self.database_object_type == 'view' or self.database_object_type == 'materialized_view':
                    # TODO: figured out a way to simplify the below crazy statements
                    # This below statement works for determing geometric MATERIALIZED view's
                    # shape field but DOES NOT work for regular geometric views.
                    stmt = '''select
                            attr.attname as column_name,
                            trim(leading '_' from tp.typname) as datatype
                            from pg_catalog.pg_attribute as attr
                            join pg_catalog.pg_class as cls on cls.oid = attr.attrelid
                            join pg_catalog.pg_namespace as ns on ns.oid = cls.relnamespace
                            join pg_catalog.pg_type as tp on tp.typelem = attr.atttypid
                            where
                            ns.nspname = '{schema}' and
                            cls.relname = '{table}' and
                            trim(leading '_' from tp.typname) = 'st_point' and
                            not attr.attisdropped and
                            cast(tp.typanalyze as text) = 'array_typanalyze' and
                            attr.attnum > 0'''.format(schema=self.schema, table=self.name)
                    target_table_shape_fields = self.db.fetch(stmt)
                    # This method works for determining geometric view's shape fields
                    # but DOES NOT work for non-geometric views.
                    # Use as a fallback.
                    if not target_table_shape_fields:
                        stmt = '''SELECT column_name FROM information_schema.columns
                            WHERE table_name = '{table_name}'
                            AND table_schema = '{table_schema}'
                            AND (data_type = 'USER-DEFINED' OR data_type = 'ST_GEOMETRY')'''.format(table_name=self.name, table_schema=self.schema)
                        target_table_shape_fields = self.db.fetch(stmt)
                # if object is not view or materialized view
                elif self.is_sde_registered:
                    print("is sde registered: ", self.is_sde_registered)
                    # First attempt to check the geom column in an SDE specific table
                    try:
                        stmt = '''select column_name from sde.st_geometry_columns where
                                        table_name = '{table_name}' '''.format(table_name=self.name)
                        target_table_shape_fields = self.db.fetch(stmt)
                    # If we're SDE enabled, but we get undefined table for sde.st_geometry_columns
                    # then we must be in RDS, where you can SDE enable a database, but the backend uses PostGIS
                    # Check for our geom column in a PostGIS table
                    except psycopg2.errors.UndefinedTable as e:
                        stmt = '''select f_geometry_column as column_name from geometry_columns 
                                                where f_table_name = '{table_name}' and f_table_schema = '{table_schema}' '''.format(table_name=self.name, table_schema=self.schema)
                        target_table_shape_fields = self.db.fetch(stmt)

            elif self.db.is_postgis_enabled: 
                # this query should work for both postgis mview and table
                stmt = '''select f_geometry_column as column_name from geometry_columns 
                                       where f_table_name = '{table_name}' and f_table_schema = '{table_schema}' '''.format(table_name=self.name, table_schema=self.schema)
                target_table_shape_fields = self.db.fetch(stmt)

            # if we find shape fields in target tables/view/materialized vies
            if not target_table_shape_fields:
                self._geom_field = None
                print("Dataset appears to be non-geometric, returning geom_field as None.")
            elif len(target_table_shape_fields) == 0:
                self._geom_field = None
                print("Dataset appears to be non-geometric, returning geom_field as None.")
            elif len(target_table_shape_fields) == 1:
                self._geom_field = target_table_shape_fields[0].pop('column_name')
                print('Geometric column detected: {}'.format(self._geom_field))
            elif len(target_table_shape_fields) > 1:
                raise LookupError('Multiple geometry fields')
            else:
                raise Exception('DB is not SDE or Postgis enabled')

        return self._geom_field



    @property
    def objectid_field(self):
        #
        f = [x['name'].lower() for x in self.metadata if 'objectid' in x['name']]
        if len(f) == 0:
            return None
        elif len(f) > 1:
            if 'objectid' in f:
                return 'objectid'
            else:
                raise LookupError('Multiple objectid fields')

        return f[0]

    def wkt_getter(self, geom_field, to_srid):
        assert geom_field is not None
        geom_getter = geom_field
        if to_srid:
            geom_getter = 'ST_Transform({}, {})'.format(geom_getter, to_srid)
        return 'ST_AsText({}) AS {}'.format(geom_getter, geom_field)

    @property
    def srid(self):
        if self._srid is None:
            if self.db.is_postgis_enabled is False and self.db.is_sde_enabled is False:
                print('DB is not SDE or Postgis enabled? Returning SRID as None')
                self._srid = None
                return self._srid
        # A database can have postgis and be ESRI SDE, e.g. an RDS database
        # So account for that by first trying SRID
            if self.db.is_postgis_enabled is True:
                stmt = "SELECT Find_SRID('{}', '{}', '{}')" \
                    .format(self.schema, self.name, self.geom_field)
                try:
                    self._srid = self.db.fetch(stmt)[0]['find_srid']
                except Exception as e:
                    if 'could not find the corresponding SRID' in str(e):
                        print('PostGIS Find_SRID() did not return SRID.')
                    else:
                        raise e
            # if we still haven't gotten an SRID and the db is sde_enabled, try is this way:
            if self.db.is_sde_enabled is True and self._srid is None:
                try:
                    stmt = "select srid from sde.st_geometry_columns where schema_name = '{}' and table_name = '{}'" \
                        .format(self.schema, self.name)
                    r = self.db.fetch(stmt)
                except:
                    stmt = "select srid from geometry_columns where f_table_name = '{}' and f_table_schema = '{}'".format(
                        self.name, self.schema)
                    r = self.db.fetch(stmt)
                if r:
                    self._srid = r[0]['srid']
                else:
                    self._srid = None
        return self._srid


    @property
    def geom_type(self):
        if self.db.is_sde_enabled is True:
            geom_dict = {1:"POINT", 9:"LINE",4:"POLYGON", 11:"MULTIPOLYGON"}
            stmt = """
                SELECT geometry_type
                FROM sde_geometry_columns
                WHERE f_table_schema = '{}'
                AND f_table_name = '{}'
                and f_geometry_column = '{}';
                """.format(self.schema, self.name, self.geom_field)
            try:
                a = self.db.fetch(stmt)
            except:
                stmt = "select geometry_type from geometry_type('{}', '{}', '{}')".format(
                    self.schema, self.name, self.geom_field)
                a = self.db.fetch(stmt)
            if a:
                geomtype = a[0].pop('geometry_type')  # this returns an int value which represents a geom type
                if type(geomtype) == int:
                    geomtype = geom_dict[geomtype]
            else:
                geomtype = None
            return geomtype
        elif self.db.is_postgis_enabled is True:
            stmt = """
                SELECT type
                FROM geometry_columns
                WHERE f_table_schema = '{}'
                AND f_table_name = '{}'
                and f_geometry_column = '{}';
                """.format(self.schema, self.name, self.geom_field)
            a = self.db.fetch(stmt)[0].pop('type')
            return a
        else:
            raise Exception('DB is not SDE or Postgis enabled??')

    @property
    def non_geom_fields(self):
        return [x for x in self.fields if x != self.geom_field]

    def query(self, fields=None, return_geom=None, geom_with_srid=None, where=None, limit=None, sql=None):
        return PostgisQuery(self.db, self, fields=fields, return_geom=return_geom,
                            geom_with_srid=geom_with_srid, where=where, limit=limit, sql=sql)

    def prepare_val(self, val, type_):
        """Prepare a value for entry into the DB."""
        if type_ == 'text':
            if val:
                val = str(val)
                # accommodate value if contains single quote for proper postgres syntax. ('roland's code -> 'roland''s code')
                val = val.replace("'", "''")
                # wrap val in single quotes
                val = "'{}'".format(val)
            else:
                val = 'NULL'
        elif type_ == 'num':
            if val is None or val =='':
                val = 'NULL'
            else:
                val = str(val)
        elif type_ == 'date':
            # TODO dates should be converted to real dates, not strings
            if val:
                val = str(val)
                val = "'{}'".format(val)
            else:
                val = 'NULL'
        elif type_ == 'geometry':
            if val:
                val = str(val)
            else:
                val='NULL'
        elif type_ == 'timestamp':
            val=str(val)
            if not val or val == 'None':
                val = 'NULL'
            elif 'timestamp' not in val.lower():
                val = '''TIMESTAMP '{}' '''.format(val)
            else:
                val = val
        elif type_ == 'timestamptz':
            val = str(val)
            if not val or val == 'None':
                val = 'NULL'
            elif 'timestamptz' not in str(val).lower():
                val = '''TIMESTAMPTZ '{}' '''.format(val)
        elif type_ == 'boolean':
            val = val if val else 'NULL'
        elif type_ == 'money':
            if not val:
                val = 'NULL'
        elif type_ == 'uuid':
            val=str(val)
        else:
            raise TypeError("Unhandled type: '{}'".format(type_))
        return val

    def _prepare_geom(self, geom, srid, transform_srid=None, multi_geom=True):
        """Prepares WKT geometry by projecting and casting as necessary."""
        # if DB is sde enabled
        if self.db.is_sde_enabled is True:
            geom = "ST_GEOMETRY('{}', {})".format(geom, srid) if geom and geom != 'EMPTY' else "null"
        # if DB is postgis enabled
        elif self.db.is_postgis_enabled is True:
            geom = "ST_GeomFromText('{}', {})".format(geom, srid) if geom and geom != 'EMPTY' else "null"
        else:
            raise Exception('DB is not SDE or Postgis enabled??')


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
            if self.db.is_sde_enabled is False:
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
        if self.database_object_type != 'table':
            raise TypeError('Database object {} is a {}, we cannot write to that!'.format(self.name,self.database_object_type))

        # Get fields from the row because some fields from self.fields may be
        # optional, such as autoincrementing integers.
        #fields from local data
        fields = rows[0]
        geom_field = self.geom_field
        objectid_field = self.objectid_field

        
        # convert rows to records (hybrid objects that can behave like dicts)
        #rows = etl.records(rows)
        # # convert rows to records (hybrid objects that can behave like dicts))
        # rows2 = etl.records(rowsnotnone)

        #rows = etl.records(rows)
        # Get geom metadata
        if geom_field:
            rowsnotnone = rows.select(geom_field, lambda v: v != None and v != '')
            first_geom_val = rowsnotnone.values(geom_field)[0] or ''
            srid = from_srid or self.srid
            match = re.match('[A-Z]+', first_geom_val)
            row_geom_type = match.group() if match else None

        # Do we need to cast the geometry to a MULTI type? (Assuming all rows
        # have the same geom type.)
        if geom_field:
            if self.geom_type.startswith('MULTI') and \
                not row_geom_type.startswith('MULTI'):
                multi_geom = True
            else:
                multi_geom = False

        #if PG objectid_field not in local data fields tuple, append to local data fields
        if objectid_field and objectid_field not in fields:
            fields = fields + (objectid_field,)
            #local_objectID_flag = True

        # Make a map of field name => type
        type_map = OrderedDict()
        for field in fields:
            try:
                type_map[field] = [x['type'] for x in self.metadata if x['name'] == field][0]
            except IndexError:
                raise ValueError('Field `{}` does not exist'.format(field))
        type_map_items = type_map.items()

        fields_joined = ', '.join(fields)
        stmt = "INSERT INTO {} ({}) VALUES ".format('.'.join([self.schema, self.name]), fields_joined)
        rows = etl.records(rows)
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

        # for each row
        for i, row in enumerate(rows):
            val_row = []
            #  for each item in a row
            for field, type_ in type_map_items:
                if type_ == 'geometry':
                    geom = row[geom_field]
                    val = self._prepare_geom(geom, srid, multi_geom=multi_geom)
                    val_row.append(val)

                # if no object id and sde enabled, use sde index to append
                elif field == objectid_field and self.db.is_sde_enabled: #and local_objectID_flag:
                    val = "sde.next_rowid('{}', '{}')".format(self.schema, self.name)
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
        if val_rows:
            vals_joined = ['({})'.format(', '.join(vals)) for vals in val_rows]
            rows_joined = ', '.join(vals_joined)
            cur_stmt += rows_joined
            execute(cur_stmt)
            commit()


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
    def __init__(self, db, table, fields=None, return_geom=True, geom_with_srid=False,
                 to_srid=None, where=None, limit=None, sql=None):
        self.db = db
        self.table = table
        self.fields = fields
        self.return_geom = return_geom
        self.geom_with_srid = geom_with_srid
        self.to_srid = to_srid
        self.where = where
        self.limit = limit
        self.sql = sql

    def __iter__(self):
        """Proxy iteration to core petl."""
        # form sql statement
        stmt = self.stmt() if not self.sql else self.sql
        # if self.sql:
        #     stmt = self.sql

        # get petl iterator
        dbo = self.db.dbo
        db_view = etl.fromdb(dbo, stmt)
        header = [h.lower() for h in db_view.header()]
        if not self.sql and self.geom_with_srid and self.table.geom_field and self.table.geom_field in header and self.table.srid:
            db_view = db_view.convert(self.table.geom_field, lambda g: 'SRID={srid};{g}'.format(srid=self.table.srid, g=g) if g not in ('', None) else '')
        iter_fn = db_view.__iter__()

        return iter_fn


    def stmt(self):
        # handle fields
        fields = self.fields
        if fields is None:
            fields = self.table.fields

        fields = [_quote(field) for field in fields]

        # handle geom
        geom_field = self.table.geom_field
        # replace geom field with wkt in fields list
        if geom_field and self.return_geom:
            wkt_getter = self.table.wkt_getter(geom_field, self.to_srid)
            geom_field_index = fields.index('"'+geom_field+'"')
            fields[geom_field_index] = wkt_getter

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
