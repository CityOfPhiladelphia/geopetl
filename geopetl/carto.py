import os
import tempfile
import petl as etl
from petl import Table


def tocarto(table, domain=None, table_name=None, api_key=None, geom_field=None):
    from carto.auth import APIKeyAuthClient
    from carto.file_import import FileImportJob
    from carto.sql import SQLClient

    temp_dir = tempfile.gettempdir()
    temp_file_path = os.path.join(temp_dir, table_name + '.csv')

    # write to csv
    # TODO can FileImport take a stream so we don't have to write to csv first?
    (etl.reproject(table, 4326, geom_field=geom_field)
        .rename('shape', 'the_geom')
        .tocsv(temp_file_path)
    )

    # import to carto
    base_url = 'https://{}.carto.com'.format(domain)
    auth_client = APIKeyAuthClient(base_url, api_key)
    file_import_job = FileImportJob(temp_file_path, auth_client)
    file_import_job.run(table_name=table_name)

    # clean up temp file
    os.remove(temp_file_path)

def _tocarto(self, *args, **kwargs):
    tocarto(self, *args, **kwargs)

Table.tocarto = _tocarto
