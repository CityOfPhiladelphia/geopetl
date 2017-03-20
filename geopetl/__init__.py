import petl as etl
from geopetl.util import parse_db_url

# activate all extensions
import geopetl.carto
import geopetl.oracle_sde
import geopetl.postgis

# activate transforms
import geopetl.transform.reproject


def _scheme_handler(url, direction):
    """Returns a from/to function for a scheme"""
    # parse url
    comps = parse_db_url(url)
    scheme = comps['scheme']

    # get 'from' function
    from_fn_name = direction + scheme
    return getattr(etl, from_fn_name)

def fromgis(url, *args, **kwargs):
    """Routes a spatial DB URL to a 'from' function"""
    try:
        handler = _scheme_handler(url, 'from')
    except AttributeError:
        raise ValueError('No matching scheme for URL: {}'.format(url))

    return handler(url, *args, **kwargs)

etl.fromgis = fromgis

def togis(table, url, *args, **kwargs):
    """Routes a spatial DB URL to a 'to' function"""
    try:
        handler = _scheme_handler(url, 'to')
    except AttributeError:
        raise ValueError('No matching scheme for URL: {}'.format(url))

    return handler(table, url, *args, **kwargs)

etl.togis = togis

def _togis(self, *args, **kwargs):
    """
    This wraps topostgis and adds a `self` arg so it can be attached to
    the Table class. This enables functional-style chaining.
    """
    return togis(self, *args, **kwargs)

etl.Table.togis = _togis
