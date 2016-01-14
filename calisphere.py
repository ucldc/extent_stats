#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" calisphere extent stats """

import sys
import argparse
import re
from datetime import date
import itertools
import json
import xlsxwriter
import requests
from pprint import pprint as pp
import ConfigParser
import time


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--pynuxrc')
    parser.add_argument('--reportrc')

    if argv is None:
        argv = parser.parse_args()

    #calisphere_extent = 

    calisphere_data = parse_calisphere(argv.reportrc)


"""
  totals -> campus -> repository -> collection
  totals -> decade 
  totals -> type
            #'repository_data',
            #'collection_data',

"""

def parse_calisphere(reportrc=None):
    config = ConfigParser.SafeConfigParser()
    config.read('report.ini')
    solr_url = config.get('calisphere', 'solrUrl')
    solr_auth = { 'X-Authentication-Token': config.get('calisphere', 'solrAuth') }
    base_query = {
        'facet': 'true',
        'facet.field': [
            'type_ss',
            'campus_data',
            'repository_data',
            'collection_data',
            'facet_decade',
        ],
        'facet.missing': 'on',
        'rows': 0,
        'facet.limit': 1000,
    }

    # non_uc_query.update({'q': '-campus_url:*',})

    run = '{}'.format(time.ctime())

    start = json.loads(requests.get(solr_url,
                                    headers=solr_auth,
                                    params=base_query,
                                    verify=False).text)

    # read from json
    campus_data = start.get('facet_counts').get('facet_fields').get('campus_data')
    repository_data = start.get('facet_counts').get('facet_fields').get('repository_data')
    collection_data = start.get('facet_counts').get('facet_fields').get('collection_data')
    facet_decade = start.get('facet_counts').get('facet_fields').get('facet_decade')
    type_ss = start.get('facet_counts').get('facet_fields').get('type_ss')

    # zip into array of arrays
    campus_data = zip(*[iter(campus_data)]*2)
    repository_data = zip(*[iter(repository_data)]*2)
    collection_data = zip(*[iter(collection_data)]*2)
    facet_decade = zip(*[iter(facet_decade)]*2)
    type_ss = zip(*[iter(type_ss)]*2)

    # open the workbook
    workbook = xlsxwriter.Workbook('combined.xlsx')
    # set up a worksheet for each page

    header_format = workbook.add_format({'bold': True, })
    number_format = workbook.add_format()
    number_format.set_num_format('#,##0')
    
    campus = workbook.add_worksheet('Campus')
    # headers
    campus.write(0, 0, 'row', header_format)
    campus.write(0, 1, 'count', header_format)
    campus.write(0, 2, 'campus', header_format)
    campus.write(0, 3, 'campus_url', header_format)
    # width
    campus.set_column(0, 0, 3, )
    campus.set_column(0, 1, 10, )
    campus.set_column(1, 2, 20, )
    campus.set_column(2, 3, 43, )
    row = 1
    for item in campus_data:
        campus.write_number(row, 0, row)
        campus.write_number(row, 1, item[1], number_format)
        if item[0]:
            cd = item[0].split('::')
            campus.write(row, 2, cd[1])
            campus.write(row, 3, cd[0])
        else:
            campus.write(row, 2, 'non-uc')
        row = row + 1
    campus.write_formula(row, 1, '=SUM(B2:B{})'.format(row))
    campus.write(row, 2, run)

    repository = workbook.add_worksheet('Repository')
    repository.write(0, 0, 'row', header_format)
    repository.write(0, 1, 'count', header_format)
    repository.write(0, 2, 'repository', header_format)
    repository.write(0, 3, 'campus', header_format)
    repository.write(0, 4, 'repository_url', header_format)
    repository.set_column(0, 0, 3, )
    repository.set_column(0, 1, 10, )
    repository.set_column(1, 2, 45, )
    repository.set_column(2, 3, 20, )
    repository.set_column(3, 4, 43, )
    row = 1
    for item in repository_data:
        repository.write_number(row, 0, row)
        repository.write_number(row, 1, item[1], number_format)
        if item[0]:
            rd = item[0].split('::')
            repository.write(row, 2, rd[1])
        else:
            repository.write(row, 2, '')
        if len(rd) == 3:
            repository.write(row, 3, rd[2])
        else:
            repository.write(row, 3, 'non-uc')
        repository.write(row, 4, rd[0])
        row = row + 1
    repository.write_formula(row, 1, '=SUM(B2:B{})'.format(row))
    repository.write(row, 2, run)

    collection = workbook.add_worksheet('Collection')
    # headers
    collection.write(0, 0, 'row', header_format)
    collection.write(0, 1, 'count', header_format)
    collection.write(0, 2, 'collection', header_format)
    collection.write(0, 3, 'collection_url', header_format)
    # width
    collection.set_column(0, 0, 3, )
    collection.set_column(0, 1, 10, )
    collection.set_column(1, 2, 45, )
    collection.set_column(2, 3, 45, )
    row = 1
    for item in collection_data:
        collection.write_number(row, 0, row)
        collection.write_number(row, 1, item[1], number_format)
        if item[0]:
            cd = item[0].split('::')
            collection.write(row, 2, cd[1])
            collection.write(row, 3, cd[0])
        row = row + 1
    collection.write_formula(row, 1, '=SUM(B2:B{})'.format(row))
    collection.write(row, 2, run)

    decade = workbook.add_worksheet('Decade')
    decade.write(0, 0, 'row', header_format)
    decade.write(0, 1, 'count', header_format)
    decade.write(0, 2, 'decade', header_format)
    row = 1
    for item in facet_decade:
        decade.write_number(row, 0, row)
        decade.write_number(row, 1, item[1], number_format)
        decade.write(row, 2, item[0])
        row = row + 1
    decade.write_formula(row, 1, '=SUM(B2:B{})'.format(row))
    decade.write(row, 2, run)

    type_wb = workbook.add_worksheet('Type')
    type_wb.write(0, 0, 'row', header_format)
    type_wb.write(0, 1, 'count', header_format)
    type_wb.write(0, 2, 'type', header_format)
    row = 1
    for item in type_ss:
        type_wb.write_number(row, 0, row)
        type_wb.write_number(row, 1, item[1], number_format)
        type_wb.write(row, 2, item[0])
        row = row + 1
    type_wb.write_formula(row, 1, '=SUM(B2:B{})'.format(row))
    type_wb.write(row, 2, run)

    workbook.close()

#def calisphere_stats():

"""

 * query 

"""


# main() idiom for importing into REPL for debugging
if __name__ == "__main__":
    sys.exit(main())


"""
Copyright © 2016, Regents of the University of California
All rights reserved.
Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:
- Redistributions of source code must retain the above copyright notice,
  this list of conditions and the following disclaimer.
- Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.
- Neither the name of the University of California nor the names of its
  contributors may be used to endorse or promote products derived from this
  software without specific prior written permission.
THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.
"""
