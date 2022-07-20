#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" calisphere extent stats """
from __future__ import print_function
import sys
import argparse
import re
from datetime import date
import itertools
import json
import requests
from pprint import pprint as pp
import ConfigParser
import time
import datetime
import os
requests.packages.urllib3.disable_warnings()


def main(argv=None):
    parser = argparse.ArgumentParser()

    if argv is None:
        argv = parser.parse_args()

    config = ConfigParser.SafeConfigParser()
    config.read('report.ini')

    solr_url = config.get('calisphere', 'solrUrl')
    solr_auth = { 'X-Authentication-Token': config.get('calisphere', 'solrAuth') }


    base_query = {
	'fl': 'id',  # fl = field list
        'rows': 1000,
        'sort': 'score desc,id desc',
    }

    ids = get_iter(solr_url, solr_auth, base_query)
    for ID in ids:
        ark = ID.get('id')
        print(u'https://calisphere.org/item/{0}/'.format(ark), )
        # print(u'https://calisphere.org/item/{0}/'.format(ark), file=outfile)


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
