from argparse import ArgumentParser
from config import TABLES
from functools import partial
from glob import iglob
from multiprocessing.dummy import Pool as ThreadPool
from numpy import int64
from os import environ, getcwd, unlink
from os.path import basename, exists, join
from pandas import DataFrame, to_datetime, to_numeric
from pyhs2 import connect
from re import sub
from shutil import copyfileobj
from time import time

from gcputils import push_to_bq, push_to_storage


def get_args():
    parser = ArgumentParser(description='HIVE extractor & exporter')
    parser.add_argument('--db', required=True, type=str, help='hive database')
    parser.add_argument('--host', default='', type=str, help='hive connection host')
    parser.add_argument('--creds', default='', type=str, help='username,password')
    parser.add_argument('--tables', default='', type=str, help='table name(s) to extract, use comma if many')
    parser.add_argument('--bucket-name', default='test', type=str, help='gcs bucket name, defaults to `test`')
    parser.add_argument('--work-dir', default=getcwd(), type=str, help='work directory, defaults to current')
    parser.add_argument('--expected-ext', default='.csv.gz', type=str, help='output extension, defaults to .csv.gz')
    return parser.parse_args()


def concat_bin(fp_pat):
    file_name = fp_pat.format('')

    try:
        with open(file_name, 'wb') as f:
            for f in iglob(fp_pat.format('_*')):
                with open(f, 'rb') as src:
                    copyfileobj(src, f)
    except Exception as e:
        print '\tsaving `{}` {}'.format(basename(file_name), e)
        return 0

    return 1


def save_slice(fp, df, schema):
    date_idx = list(map(lambda x: x.endswith('DATETIME'), schema))
    df.iloc[:, date_idx] = df.iloc[:, date_idx].applymap(\
        lambda x: to_datetime(x, errors='coerce'))

    float_idx = list(map(lambda x: x.endswith('FLOAT'), schema))
    df.iloc[:, float_idx] = df.iloc[:, float_idx].applymap(\
        lambda x: to_numeric(x, errors='coerce'))

    int_idx = list(map(lambda x: x.endswith('INTEGER'), schema))
    df.iloc[:, int_idx] = df.iloc[:, int_idx].fillna(0).applymap(int64)

    str_idx = list(map(lambda x: x.endswith('STRING'), schema))
    df.iloc[:, str_idx] = df.iloc[:, str_idx].applymap(\
        lambda x: sub('\r|"', '', x) if x and type(x) is str else x)

    try:
        df.to_csv(fp, header=False, index=False, encoding='utf8')
    except Exception as e:
        print('\tsaving slice `{}` {}'.format(basename(fp), e))

    pass


def save_schema(table, cursor, work_dir):
    schema_name = '_'.join([table, 'schema.csv'])
    types = cursor.getSchema()

    type_map = {
        'BIGINT_TYPE': 'INTEGER',
        'INTEGER_TYPE': 'INTEGER',
        'SMALLINT_TYPE': 'INTEGER',
        'BINARY_TYPE': 'BOOLEAN',
        'DOUBLE_TYPE': 'FLOAT',
        'REAL_TYPE': 'FLOAT',
        'FLOAT_TYPE': 'FLOAT',
        'NUMERIC_TYPE': 'FLOAT',
        'DECIMAL_TYPE': 'FLOAT',
        'STRING_TYPE': 'STRING',
        'VARCHAR_TYPE': 'STRING',
        'CHAR_TYPE': 'STRING',
        'TIMESTAMP_TYPE': 'DATETIME',
        'TIME': 'DATETIME',
        'DATE': 'DATETIME'
    }

    fields = ['{}:{}'.format(dt['columnName'][dt['columnName'].find('.')+1:], \
        type_map.get(dt['type'], 'STRING')) for dt in types]

    try:
        with open(join(work_dir, schema_name), 'w') as f:
            f.write(','.join(fields))
    except Exception as e:
        print '\tsaving schema `{}` {}'.format(table, e)

    return fields


def fetch_data(curs, table, work_dir, ext):
    err = None
    try:
        curs.execute('SELECT * FROM ' + table)
    except Exception as e:
        err = str(e)
    else:
        schema = save_schema(table, curs, work_dir)

    i = 1
    while not err and curs.hasMoreRows:
        file_path = '_'.join([join(work_dir, table), str(i) + ext])
        print '\tfetching {} of `{}`'.format(curs.MAX_BLOCK_SIZE * i, table)

        try:
            df = DataFrame.from_records(curs.fetchSet())
        except Exception as e:
            err = str(e)
        else:
            save_slice(file_path, df, schema) if not df.empty else None

        i += 1

    print '\tfetching `{}` {}'.format(table, err) if err else None
    return 0 if err else 1


def get_cursor(cursor, c, get_new_conn):
    max_retry = 3
    fails = 0

    try:
        cursor.close()
    except:
        cursor = None

    conn = None
    while fails < max_retry:
        try:
            conn = get_new_conn() if not c or fails else c
            cursor = conn.cursor()
        except Exception as e:
            print '\tgetting cursor {}'.format(e)
            fails += 1
        else:
            break

    return cursor, conn


def extract_all(tables, get_new_conn, work_dir, ext):
    curs = conn = None
    extracted = []

    for tbl in tables:
        path_pat = join(work_dir, tbl) + '{}' + ext
        curs, conn = get_cursor(curs, conn, get_new_conn)

        res = fetch_data(curs, tbl, work_dir, ext) and concat_bin(path_pat) \
            if curs and conn else 0

        extracted.append(tbl) if res else None
        print '\textracting `{}` [{}]'.format(tbl, 'OK' if res else 'FAIL')

        p = ThreadPool()
        p.map(unlink, iglob(join(work_dir, tbl) + '_*' + ext))

        p.close()
        p.join()

    return extracted


def export_all(exported_to_gcs, db, ext):
    c = None
    def f(item):
        blob_path, file_path, err = item
        table = basename(file_path).replace(ext, '')

        schema_name = '_'.join([table, 'schema.csv'])
        schema_path = join(dirname(file_path), schema_name)

        c, err, succeeds = push_to_bq(c, db, table, blob_path, schema_path) \
            if blob_path else (c, '\texporting to gcs `{}` {}'.format(table, err), False)

        print err
        return blob_path if succeeds else None

    p = ThreadPool()
    exported = filter(None, p.map(f, exported_to_gcs))

    p.close()
    p.join()

    return list(exported)


def export():
    args = get_args()
    begin = time()

    tables = set(args.tables.split(','))
    tables = TABLES[args.db] if not tables and args.db in TABLES else ['']

    files = [join(args.work_dir, t + args.expected_ext) for t in tables \
        if exists(join(args.work_dir, t + args.expected_ext))]

    f = partial(push_to_storage, bucket_name=args.bucket_name, blob_name=args.db)
    p = ThreadPool()

    print 'Start exporting `{}`'.format(args.db)
    exported_to_gcs = p.map(f, files)

    p.close()
    p.join()

    exported = export_all(exported_to_gcs, args.db, args.expected_ext)
    res = 0 if len(tables) == len(exported) else 1

    print 'Export all {0:.2f} s, [{1}]'.format(time()-begin, 'FAIL' if res else 'OK')
    return res


def extract():
    args = get_args()
    begin = time()

    tables = TABLES.get(args.db, [])
    tables = set(tables) & set(args.tables.split(',') + list(tables))

    print 'Start extracting `{}` {}'.format(args.db, ','.join(tables))
    creds = args.creds.split(',')

    get_new_conn = lambda: connect(port=10000, authMechanism='PLAIN', ssl=True, \
        database=args.db, host=args.host, user=creds[0], password=','.join(creds[1:]))

    extracted = extract_all(tables, get_new_conn, args.work_dir, args.expected_ext)

    res = 0 if len(tables) and len(tables) == len(extracted) else 1 \
        if len(extracted) else -1

    print 'Extract all {0:.2f} s, [{1}]'.format(time()-begin, 'FAIL' if res else 'OK')
    return res


if __name__ == '__main__':
    res = extract()
    res = export() if res >= 0 else 1
    quit(res)