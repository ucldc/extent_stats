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
import datetime
import os


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('outdir', nargs=1,)

    if argv is None:
        argv = parser.parse_args()

    run = '{}'.format(time.ctime())

    #calisphere_extent = 
    today = datetime.date.today()
    fileout = os.path.join(argv.outdir[0], '{}-dpla.xlsx'.format(today))
    config = ConfigParser.SafeConfigParser()
    config.read('report.ini')
    dpla_base = config.get('dpla', 'base')
    dpla_key = config.get('dpla', 'api_key')
    dpla_results = dpla_query(dpla_base, dpla_key)
    dpla_counts = dpla_results.get('facets').get('dataProvider').get('terms')
    workbook = xlsxwriter.Workbook(fileout)
    worksheet = workbook.add_worksheet('dp.la')
    row = 1
    for line in dpla_counts:
        worksheet.write_number(row, 0, row)
        worksheet.write_number(row, 1, line['count'])
        worksheet.write(row, 2, line['term'])
        row = row + 1
    worksheet.write_formula(row, 1, '=SUM(B2:B{})'.format(row))
    worksheet.write(row, 2, run)


def dpla_query(base_url, api_key):
    params = {
        'provider.name': 'California Digital Library',
        'facets': 'dataProvider',
        'facet_size': 2000,
        'api_key': api_key,
        'page_size': 0,
    }
    res = requests.get(url=base_url, params=params)
    try:
        res.raise_for_status()
    except requests.exceptions.HTTPError:
        return False
    return json.loads(res.text)


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
