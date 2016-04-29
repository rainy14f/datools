# -*- coding: utf-8 -*-


import csv, sqlite3
import logging


def parse_url(url):
    import re
    skuid_search = re.search('skuId=(?P<skuId>[0-9]+)', url)
    id_search = re.search('id=(?P<id>[0-9]+)', url)
    skuid = skuid_search.group('skuId') if skuid_search else None
    id = id_search.group('id') if id_search else None
    rpc = skuid if skuid else id
    return skuid, id, rpc


def classify(file_path):
    id_rows = []
    id_skuid_rows = []
    header = []

    with open(file_path, 'rb') as fin:
        reader = csv.reader(fin)
        header = reader.next()  # skip header
        header.insert(23, 'id')
        header.insert(24, 'skuId')
        header.insert(25, 'RPC')

        for line in reader:
            skuid, id, rpc = parse_url(line[3])
            line.insert(23, id)
            line.insert(24, skuid)
            line.insert(25, rpc)
            if id and skuid:
                id_skuid_rows.append(line)
            elif id and not skuid:
                id_rows.append(line)
            else:
                print 'Error: %s' % line[3]

    return id_rows, id_skuid_rows, header


def analyse(file_path_prev, file_path):

    prev_id, prev_id_skuid, header = classify(file_path_prev)
    this_id, this_id_skuid, header = classify(file_path)

    wrong_rows = []
    for ref_row in prev_id:
        if [row for row in prev_id_skuid if row[23] == ref_row[23]]:
            wrong_rows.append(ref_row)

    if wrong_rows:
        print 'Please review these rows (see IDs below):'
        for r in wrong_rows:
            print r[23]
        return None, None


    prev_remove = []
    for prev_row in prev_id:
        if [row for row in this_id_skuid if row[23] == prev_row[23]]:
            prev_remove.append(prev_row)

    for prev_row in prev_id_skuid:
        if [row for row in this_id if row[23] == prev_row[23]]:
            prev_remove.append(prev_row)

    this_add = []
    for this_row in this_id:
        if not [row for row in prev_id if row[23] == this_row[23]]:
            this_add.append(this_row)

    for this_row in this_id_skuid:
        if not [row for row in prev_id_skuid if row[23] == this_row[23] and row[24] == this_row[24]]:
            this_add.append(this_row)

    print 'Add: %d' % len(this_add)
    print 'Remove: %d' % len(prev_remove)

    prev_remove.insert(0, header)
    this_add.insert(0, header)
    return prev_remove, this_add


def write_file(file_path, lines):
    with open(file_path, 'wb') as fout:
        writer = csv.writer(fout)
        for line in lines:
            writer.writerow(line)


import sys
reload(sys)
sys.setdefaultencoding('utf-8')


remove, add = analyse('c:\\dev\\Tmall_Dove_byCategory_all.csv', 'c:\\dev\\Tmall_Dove_byCategory_2016-04.csv')
if remove and add:
    write_file('c:\\dev\\out_remove.csv', remove)
    write_file('c:\\dev\\out_add.csv', add)

