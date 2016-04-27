# -*- coding: utf-8 -*-


import csv, sqlite3
import logging


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
    for line in f:
        #yield line.encode("ascii", "xmlcharrefreplace").decode("ascii")
        url = line[3]
        import re
        skuid_search = re.search('skuId=(?P<skuId>[0-9]+)', url)
        id_search = re.search('id=(?P<id>[0-9]+)', url)
        skuid = skuid_search.group('skuId') if skuid_search else None
        id = id_search.group('id') if id_search else None
        rpc = skuid if skuid else id
        line.insert(23, skuid)
        line.insert(24, id)
        line.insert(25, rpc)
        yield line



def csvToDb(csvFile, outputToFile = False):
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
        cols.insert(23, 'SkuID')
        cols.insert(24, 'ID')
        cols.insert(25, 'RPC')

        table_name = 'ads7'

        # Generate create table statement:
        stmt = "CREATE TABLE %s (%s)" % (table_name, ",".join(cols))

        con = sqlite3.connect("f:/dev/csv.db")
        con.text_factory = lambda x: unicode(x, "utf-8", "ignore")
        cur = con.cursor()
        cur.execute(stmt)

        fin.next()

        reader = csv.reader(fin)

        # Generate insert statement:
        stmt = "INSERT INTO %s VALUES(%s);" % (table_name, ','.join('?' * len(cols)))

        cur.executemany(stmt, insert_ids(reader))
        con.commit()

import sys
reload(sys)
sys.setdefaultencoding('utf-8')

csvToDb('f:\\Tmall_Skii_by_brand.csv')
