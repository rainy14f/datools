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

        cur.executemany(stmt, reader)
        con.commit()


import sys
reload(sys)
sys.setdefaultencoding('utf-8')

csv_to_db('f:\\Tmall_Dove_byCategory-2016-03.csv')


