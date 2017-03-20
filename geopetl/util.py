import petl as etl

# get urlparse based on py version
if etl.compat.PY2:
    from urlparse import urlparse
else:
    from urllib.parse import urlparse

def parse_db_url(url):
    """Convenience function for parsing SQLAlchemy-style DB URLs."""
    parsed = urlparse(url)

    # check for valid-ish url
    scheme = parsed.scheme
    if scheme in [None, '']:
        raise ValueError('Not a valid URL')

    # convert generic (sqlalchemy-style) names to spatial ones; remove special chars
    scheme = scheme.replace('postgresql', 'postgis')\
                   .replace('-', '')\
                   .replace('_', '')

    # get db and table name, if available
    path = parsed.path
    path_parts = path.split('/')
    db_name = path_parts[1] if len(path_parts) > 1 else None
    table_name = path_parts[2] if len(path_parts) > 2 else None

    return {
        'scheme':       scheme,
        'host':         parsed.hostname,
        'port':         parsed.port,
        'user':         parsed.username,
        'password':     parsed.password,
        'db_name':      db_name,
        'table_name':   table_name,
    }
    
