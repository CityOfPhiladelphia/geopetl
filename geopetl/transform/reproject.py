import petl as etl
from petl import Table
from petl.compat import text_type
from geopetl.base import SpatialQuery

class ReprojectView(Table):
    def __init__(self, source, to_srid, from_srid=None, geom_field=None):
        self.source = source
        self.to_srid = to_srid
        self.geom_field = geom_field or source.geom_field
        self.from_srid = source.table.srid

        # lazy load shapely & pyproj
        from geopetl.transform.reproject_util import WktTransformer
        self.tsf = WktTransformer(self.from_srid, to_srid)

    def __iter__(self):
        source = self.source
        it = iter(source)
        hdr = next(it)
        flds = list(map(text_type, hdr))
        yield tuple(hdr)  # these are not modified

        # get geom field
        # TODO this means you can only call reproject directly after fromoraclesde(), etc
        geom_field = self.geom_field
        geom_field_i = flds.index(geom_field)

        for row in it:
            _row = list(row)
            geom = row[geom_field_i]
            geom_t = self.tsf.transform(geom)
            _row[geom_field_i] = geom_t

            yield tuple(_row)

def reproject(*args, **kwargs):
    return ReprojectView(*args, **kwargs)

etl.reproject = reproject
SpatialQuery.reproject = reproject
