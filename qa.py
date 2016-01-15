#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" qa script for ucsf """

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
import os
import logging
from pynux import utils


def main(argv=None):

    parser = argparse.ArgumentParser(description='nuxeo metadata via REST API')
    parser.add_argument('path', nargs=1, help="nuxeo document path")
    utils.get_common_options(parser)
    if argv is None:
        argv = parser.parse_args()

    nx = utils.Nuxeo(rcfile=argv.rcfile, loglevel=argv.loglevel.upper())
    documents = nx.children(argv.path[0])


    # open the workbook
    workbook = xlsxwriter.Workbook('qa.xlsx')
    header_format = workbook.add_format({'bold': True, })

    report = workbook.add_worksheet()

    report.set_column(0, 0, 10, )
    report.set_column(1, 2, 40, )
    report.set_column(3, 4, 80, )

    report.write(0, 0, 'nuxeo-uid', header_format)
    report.write(0, 1, 'ucldc_schema:localidentifier', header_format)
    report.write(0, 2, 'filename', header_format)
    report.write(0, 3, 'nuxeo-path', header_format)
    report.write(0, 4, 'title', header_format)

    # document specified on command line
    root_doc = nx.get_metadata(path=argv.path[0])

    report.write(1, 0, root_doc['uid'])
    report.write(1, 3, argv.path[0])

    row = 2
    for document in documents:

        p = document['properties']
        
        report.write(row, 0, document['uid'])
        report.write(row, 1, p['ucldc_schema:localidentifier'][0])
        if 'picture:views' in p:
            imported_file = p['picture:views'][1]['filename']
            report.write(row, 2, imported_file)
        report.write(row, 3, document['path'].replace(argv.path[0], '', 1))
        report.write(row, 4, document['title'])
        row = row + 1


    # output
    #  path|localid|title
    #

    workbook.close()


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
