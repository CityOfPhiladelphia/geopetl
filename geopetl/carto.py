import petl as etl
from petl import Table


def tocarto(table, url, geom_field=None):
    from cartodb import CartoDBAPIKey, CartoDBException, FileImport

    filename = name + '.csv'

    # write to csv
    # TODO can FileImport take a stream so we don't have to write to csv first?
    (etl.reproject(table, 4326, geom_field=geom_field)
        .rename('shape', 'the_geom')
        .tocsv(filename)
    )

    API_KEY = '<api key>'
    DOMAIN = '<domain>'
    cl = CartoDBAPIKey(API_KEY, DOMAIN)
    fi = FileImport(filename, cl, privacy='public')
    fi.run()

def _tocarto(self, *args, **kwargs):
    tocarto(self, *args, **kwargs)

Table.tocarto = _tocarto
