#!/usr/bin/env python3
# -*- coding: utf-8 -*-
""" calisphere extent stats """

import sys
import argparse
import re
from datetime import date
import itertools
import json
import requests
from pprint import pprint as pp
import configparser
import time
import datetime
import os
requests.packages.urllib3.disable_warnings()


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('institution_number', type=int, nargs=1,)

    if argv is None:
        argv = parser.parse_args()

    config = configparser.ConfigParser()
    config.read('report.ini')

    solr_url = config.get('calisphere', 'solrUrl')
    solr_auth = { 'X-Authentication-Token': config.get('calisphere', 'solrAuth') }

# https://registry.cdlib.org/api/v1/repository/
# curl ... "https://solr.calisphere.org/solr/query?q=repository_data:*/87/*" | grep repository

    repository_query = u"repository_url:https://registry.cdlib.org/api/v1/repository/{}/".format(argv.institution_number[0])

    print(repository_query)

# {'creator': ['Federal Energy Regulatory Commission'],
# 'date': ['10/22/02'],
# 'id': 'ark:/86086/n2nk3dps',
# 'relation': ['http://elibrary.ferc.gov/idmws/search/intermediate.asp?link_desc=yes&slcfilelist=9622598:0'],
# 'title': ['Environmental Inspection Report by Chicago Regional Office for '
#           "Traverse City Light and Power's Sabin Project"]}

    base_query = {
        'q': 'identifier:"ark:/" AND {}'.format(repository_query),
	'fl': 'id, collection_url, title, creator, date',  # fl = field list
        'rows': 1000,
        'sort': 'score desc,id desc',
    }

    outfile=sys.stdout

    ids = get_iter(solr_url, solr_auth, base_query)
    for ID in ids:
        ark = ID.get('id')
        relation = ID.get('collection_url')
        title = ID.get('title')
        creator = ID.get('creator')
        date = ID.get('date')
        relation = cleanup(relation)
        title = cleanup(title)
        creator = cleanup(creator)
        date = cleanup(date)

        print('{0}\thttps://calisphere.org/item/{0}/\t{1}\t{2}\t{3}\t{4}'.format(ark, relation, title, creator, date,), file=outfile)


def cleanup(solr_key):
    try:
        return solr_key[0]
    except TypeError:
        return '(:unav)'


def get_page(url, headers, params, cursor='*'):
    params.update({'cursorMark': cursor})
    res = requests.get(url, headers=headers, params=params, verify=False)
    res.raise_for_status()
    return json.loads(res.content)


def get_iter(url, headers, params):
    nextCursorMark = '*'
    while True:
        results_dict = get_page(url, headers, params, nextCursorMark)
        if len(results_dict['response']['docs']) == 0:
            break
        for document in results_dict['response']['docs']:
            yield document
        nextCursorMark = results_dict.get('nextCursorMark', False)


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
