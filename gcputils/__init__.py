from fnmatch import fnmatch
from google.auth import default
from google.cloud import bigquery as bq, storage as gs, exceptions as gcp_exc
from logging import error
from os.path import basename, dirname, join
from re import sub
from warnings import simplefilter
simplefilter('ignore', UserWarning)


def get_credentials():
    scopes = ('https://www.googleapis.com/auth/cloud-platform',)

    try:
        return default(scopes=scopes)
    except Exception as e:
        error(str(e))

    return (None,'')


def get_schema_fields(file_path):
    fields = []
    try:
        with open(file_path, 'r') as f:
            schema = f.read()
        columns = [col.strip() for col in schema.split(',') if col]
    except Exception as e:
        error('read schema `{}` {}'.format(file_path, e))
        columns = []

    def get_bq_name(name):
        cn = name.replace('"', '')
        cn = sub('(^\d+)', '_\\1', cn)
        cn = sub('\W', '_', cn)
        return cn[:120]

    for col in columns:
        ind = col.rfind(':')
        fields.append(bq.SchemaField(get_bq_name(col[:ind]), col[ind+1:]))

    return fields


def get_extract_config(file_name):
    jc = bq.ExtractJobConfig()

    compression = bq.job.Compression.GZIP if fnmatch(file_name, '*.gz') \
        else bq.job.Compression.NONE

    jc.compression = compression
    jc.destination_format = bq.job.DestinationFormat.CSV

    return jc


def get_load_config(file_path):
    jc = bq.LoadJobConfig()
    jc.skip_leading_rows = 0

    jc.allow_quoted_newlines = True
    jc.source_format = bq.ExternalSourceFormat.CSV

    jc.write_disposition = bq.job.WriteDisposition.WRITE_TRUNCATE
    jc.schema = get_schema_fields(file_path)

    if not jc.schema:
        jc.autodetect = True

    return jc


def get_bq_entity(client, dataset_id, table_id=''):
    creds, proj = get_credentials()
    entity_id = '.'.join(filter(None, [proj, dataset_id, table_id]))

    try:
        c = bq.Client(project=proj, credentials=creds) \
            if not client else client
        entity = c.create_table(entity_id, exists_ok=True) \
            if table_id else c.create_dataset(entity_id, exists_ok=True)
    except Exception as e:
        return (client,None,str(e))

    return (c,entity,'')


def push_to_bq(c, dataset_id, table_id, blob_path, schema_path):
    jc = get_load_config(schema_path)
    c, table, err = get_bq_entity(c, dataset_id, table_id)

    max_retry=3
    fails = 0

    while table and fails < max_retry:
        try:
            job = c.load_table_from_uri(blob_path, table.reference, job_config=jc)
            job.result()
        except Exception as e:
            err = str(e)
            fails += 1
        else:
            err = '' 
            break

    err = err or 'OK' if fails < max_retry else err or 'FAIL'
    return (c,'\texporting to gbq `{}` {}'.format(table_id, err), err == 'OK')


def get_gcs_bucket(bucket_name):
    creds, proj = get_credentials()

    try:
        c = gs.Client(project=proj, credentials=creds)
        bck = c.get_bucket(bucket_name)
    except gcp_exc.NotFound:
        bck = c.create_bucket(bucket_name)
    except Exception as e:
        return (None,str(e))

    return (bck,'')


def push_to_storage(file_path, bucket_name, blob_name=''):
    max_retry=3
    fails = 0

    chunk_size = 1<<23
    bck, err = get_gcs_bucket(bucket_name)

    if err: 
        return ('', file_path, err)

    if blob_name:
        blob_name = '/'.join([blob_name, basename(file_path)])

    gs.blob._MAX_MULTIPART_SIZE = chunk_size
    blob = gs.Blob(blob_name, bck, chunk_size)

    while blob and fails < max_retry:
        try:
            with open(file_path, 'rb') as f:
                blob.upload_from_file(f)
        except Exception as e:
            err = str(e)
            fails += 1
        else:
            err = ''
            break

    blob_path = 'gs://{}/{}'.format(bck.name, blob.name) \
        if fails < max_retry else ''

    return (blob_path, file_path, err)


def extract_bq_table(dataset_id, table_id, blob_path, timeout=1800):
    jc = get_extract_config(blob_path)
    pfx = '/'.join([dataset_id, table_id])

    c = None
    c, table, err = get_bq_entity(c, dataset_id, table_id)

    if err:
        return ('\textracting bq table `{}` {}'.format(pfx, err), None)

    if blob_path and not blob_path.startswith('gs://'):
        blob_path = 'gs://' + blob_path

    try:
        job = c.extract_table(table, blob_path, job_config=jc)
        job.result(timeout=timeout)
    except Exception as e:
        return ('\textracting bq table `{}` {}'.format(pfx, e), None)

    schema_query = "SELECT string_agg(concat(column_name,':',data_type),',') AS schema \
        FROM `{}.INFORMATION_SCHEMA.COLUMNS` WHERE table_name='{}'".format(dataset_id, table_id)

    try:
        schema = c.query(schema_query).to_dataframe().loc[0, 'schema']
    except:
        schema = ''

    return ('/'.join([job.job_id, job.state]), schema)