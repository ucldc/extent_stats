#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import argparse
import os
from pynux import utils
import xlsxwriter
import time

from pprint import pprint as pp


def main(argv=None):

    parser = argparse.ArgumentParser(description='extent stats via Nuxeo REST API')
    parser.add_argument('nxql', nargs=1, help="nxql query")
    parser.add_argument('-x', '--xlsx', 
        help="xlsx")
    utils.get_common_options(parser)
    if argv is None:
        argv = parser.parse_args()

    nx = utils.Nuxeo(rcfile=argv.rcfile, loglevel=argv.loglevel.upper())

    documents = nx.nxql('select * from Document where ecm:path startswith"{0}"'.format(argv.nxql[0]))

    # open the workbook
    workbook = xlsxwriter.Workbook(argv.xlsx)
    # set up a worksheet for each page

    header_format = workbook.add_format({'bold': True, })
    number_format = workbook.add_format()
    number_format.set_num_format('#,##0')
    
    campus = workbook.add_worksheet('Campus')
    # headers
    campus.write(0, 0, 'row', header_format)
    campus.write(0, 1, 'uid', header_format)
    campus.write(0, 2, 'path', header_format)
    campus.write(0, 3, 'xpath', header_format)
    campus.write(0, 4, 'name', header_format)
    campus.write(0, 5, 'data-url', header_format)
    campus.write(0, 6, 'md5', header_format)
    campus.write(0, 7, 'bytes', header_format)
    campus.write(0, 8, 'mime-type', header_format)
    # width
    campus.set_column(0, 0, 3, )
    campus.set_column(1, 1, 10, )
    campus.set_column(2, 2, 40, )
    campus.set_column(3, 3, 5, )
    campus.set_column(4, 4, 20, )
    campus.set_column(5, 5, 5, )
    campus.set_column(6, 6, 20, )
    campus.set_column(7, 7, 10, )
    campus.set_column(8, 8, 10, )

    row = 1 
    for document in documents:
        for blob in blob_from_doc(document):
            if blob:
                campus.write(row, 0, row, number_format)
                campus.write(row, 1, blob['uid'],)
                campus.write(row, 2, blob['path'],)
                campus.write(row, 3, blob['xpath'],)
                campus.write(row, 4, blob['name'],)
                campus.write(row, 5, blob['data'],)
                campus.write(row, 6, blob['digest'],)
                campus.write(row, 7, int(blob['length']), number_format)
                campus.write(row, 8, blob['mime-type'],)
                row = row + 1

    campus.write_formula(row, 7, '=SUM(H2:H{})'.format(row))
    run = '{}'.format(time.ctime())
    campus.write(row, 2, run)


def blob_from_doc(document):
    blobs = []
    if 'file:content' in document['properties'] and document['properties']['file:content']:
        main_file = document['properties']['file:content']
        main_file[u'xpath'] = 'file:content'
        main_file[u'uid'] = document['uid']
        main_file[u'path'] = document['path']
        blobs.append(main_file)
    if 'extra_files:file' in document['properties']:
        for idx, blob in enumerate(document['properties']['extra_files:file']):
            if blob['blob']:
                blob['blob'][u'xpath'] = 'extra_files:file/blob[{0}]'.format(idx + 1)
                blob['blob'][u'uid'] = document['uid']
                blob['blob'][u'path'] = document['path']
                blobs.append(blob['blob'])
    return blobs



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
