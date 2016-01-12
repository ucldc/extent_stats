#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" facet decade in python """

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


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--pynuxrc')
    parser.add_argument('--reportrc')

    if argv is None:
        argv = parser.parse_args()

    #calisphere_extent = 

    calisphere_data = parse_calisphere(argv.reportrc)

def parse_calisphere(reportrc=None):
    config = ConfigParser.SafeConfigParser()
    config.read('report.ini')
    solr_url = config.get('calisphere', 'solrUrl')
    solr_auth = { 'X-Authentication-Token': config.get('calisphere', 'solrAuth') }
    base_query = {
        'facet': 'true',
        'facet.field': [ 'repository_data',
                         'collection_data',
                         'type_ss',
                         'campus_url', ],
        'rows': 0,
        'facet.sort': 'index',
        'facet.limit': 1000,
    }
    non_uc_query = base_query
    non_uc_query.update({'q': '-campus_url:*',})
    pp(non_uc_query)
    result = json.loads(requests.get(solr_url, headers=solr_auth, params=non_uc_query, verify=False).text)

    pp(result)
    

def run_report():
    print "hey"

    workbook = xlsxwriter.Workbook('hello.xlsx')
    worksheet = workbook.add_worksheet()
    worksheet.write('A1', 'Hello world')

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
