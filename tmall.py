# -*- coding: utf-8 -*-


import csv, sqlite3
import logging


DB_NAME = 'tmall_skuid.db'

def _get_col_datatypes(fin):
    dr = csv.DictReader(fin) # comma is default delimiter
    fieldTypes = {}
    for entry in dr:
        feildslLeft = [f for f in dr.fieldnames if f not in fieldTypes.keys()]
        if not feildslLeft: break # We're done
        for field in feildslLeft:
            data = entry[field]

            # Need data to decide
            # if len(data) == 0:
            #     Tmall_Skii_by_brand.csv

            if data.isdigit():
                fieldTypes[field] = "INTEGER"
            else:
                fieldTypes[field] = "TEXT"
        # TODO: Currently there's no support for DATE in sqllite

    if len(feildslLeft) > 0:
        raise Exception("Failed to find all the columns data types - Maybe some are empty?")

    return fieldTypes


def insert_ids(f):
    """
    Inser SKUID, ID, RPC in the CSV rows
    :param f: a csv.reader object
    :return: generator
    """
    for line in f:
        url = line[3]
        skuid, id, rpc = parse_url(url)
        line.insert(23, skuid)
        line.insert(24, id)
        line.insert(25, rpc)
        yield line


def parse_url(url):
    import re
    skuid_search = re.search('skuId=(?P<skuId>[0-9]+)', url)
    id_search = re.search('id=(?P<id>[0-9]+)', url)
    skuid = skuid_search.group('skuId') if skuid_search else None
    id = id_search.group('id') if id_search else None
    rpc = skuid if skuid else id
    return skuid, id, rpc


def csv_to_db(csvFile, outputToFile = False):
    # TODO: implement output to file

    with open(csvFile,mode='rb') as fin:
        dt = _get_col_datatypes(fin)

        fin.seek(0)

        reader = csv.DictReader(fin)

        # Keep the order of the columns name just as in the CSV
        fields = reader.fieldnames
        cols = []

        # Set field and type
        for f in fields:
            cols.append("%s %s" % (f, dt[f]))

        # insert IDs
        cols.insert(23, 'skuid')
        cols.insert(24, 'id')
        cols.insert(25, 'rpc')

        table_name = 'ads7'

        # Generate create table statement:
        con = sqlite3.connect(DB_NAME)
        con.text_factory = lambda x: unicode(x, "utf-8", "ignore")
        cur = con.cursor()

        cur.execute('DROP TABLE IF EXISTS %s' % (table_name))
        stmt = "CREATE TABLE %s (%s)" % (table_name, ",".join(cols))
        cur.execute(stmt)

        #fin.next()

        reader = csv.reader(fin)

        # Generate insert statement:
        stmt = "INSERT INTO %s VALUES(%s);" % (table_name, ','.join('?' * len(cols)))

        cur.executemany(stmt, insert_ids(reader))
        con.commit()


def analyse(csv_file_path):
    #con = sqlite3.connect("f:/dev/csv.db")
    con = sqlite3.connect(DB_NAME)
    cur = con.cursor()

    output = []
    with open(csv_file_path, 'rb') as fin:
        reader = csv.reader(fin)
        header = reader.next() # skip header
        header.insert(23, 'prev_id')
        header.insert(24, 'prev_skuId')
        header.insert(25, 'prev_RPC')
        header.insert(26, 'id')
        header.insert(27, 'skuId')
        header.insert(28, 'RPC')
        header.insert(29, 'Changed')
        output.append(header)

        for line in reader:
            skuid, id, rpc = parse_url(line[3])
            if skuid:
                cur.execute('select skuid, id, rpc from ads7 where skuid=:skuid', {'skuid': skuid})
                res = cur.fetchone()
                if res:
                    prev_skuid, prev_id, prev_rpc = res[0], res[1], res[2]
            else:
                cur.execute('select skuid, id, rpc from ads7 where id=:id', {'id':id})
                print cur.rowcount
                res = cur.fetchone()
                if res:
                    prev_skuid, prev_id, prev_rpc = res[0], res[1], res[2]
                else:
                    print '(skuid: %s, id: %s) is a new SKU' % (skuid, id)
                    prev_skuid, prev_id, prev_rpc = ('', '', '')

            line.insert(23, prev_id)
            line.insert(24, prev_skuid)
            line.insert(25, prev_rpc)
            line.insert(26, id)
            line.insert(27, skuid)
            line.insert(28, rpc)
            line.insert(29, 'x' if rpc!=prev_rpc else '')
            output.append(line)

    con.close()

    # write output
    import os
    fn, ext = os.path.splitext(csv_file_path)
    output_file_path = fn + '_out' + ext

    with open(output_file_path, 'wb') as fout:
        writer = csv.writer(fout)
        for line in output:
            writer.writerow(line)




import sys
reload(sys)
sys.setdefaultencoding('utf-8')

csv_to_db('f:\\Tmall_Dove_byCategory-2016-03.csv')
analyse('f:\\Tmall_Dove_byCategory-2016-04.csv')


