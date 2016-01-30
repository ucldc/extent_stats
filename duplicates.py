#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import argparse
import os
from pynux import utils
import time

from collections import defaultdict

from boto import s3
from boto.s3.connection import OrdinaryCallingFormat 
from pprint import pprint as pp


def main(argv=None):
    parser = argparse.ArgumentParser(description='extent stats via Nuxeo REST API')
    utils.get_common_options(parser)
    if argv is None:
        argv = parser.parse_args()

    nx = utils.Nuxeo(rcfile=argv.rcfile, loglevel=argv.loglevel.upper())

    documents = nx.nxql('select * from Document where ecm:path startswith "/asset-library/UCM"')

    duplicates = defaultdict(list)

    row = 0
    for document in documents:
        for blob in blob_from_doc(document):
            if blob:
                duplicates[blob['digest']].append((blob['uid'] ,u'{0}#{1}'.format(blob['path'], blob['xpath']).encode('utf-8')))
        if row % 25000 == 0:
            print '{0} blobs checked'.format(row)
        row = row + 1
    duplicates = {k: v for k, v in duplicates.items() if len(v) > 1}  # http://stackoverflow.com/a/8425075
    pp(duplicates)
    print(len(duplicates))


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
                blob['blob'][u'xpath'] = 'extra_files:file/item[{0}]/blob'.format(idx + 1)
                blob['blob'][u'uid'] = document['uid']
                blob['blob'][u'path'] = document['path']
                blobs.append(blob['blob'])
    if 'picture:views' in document['properties']:
        for idx, blob in enumerate(document['properties']['picture:views']):
            blob['content'][u'xpath'] = 'picture:views/item[{0}]/content'.format(idx + 1)
            blob['content'][u'uid'] = document['uid']
            blob['content'][u'path'] = document['path']
            blob['content'][u'name'] = blob['filename']
            blobs.append(blob['content'])
    if 'vid:storyboard' in document['properties']:
        for idx, blob in enumerate(document['properties']['vid:storyboard']):
            b = blob['content']
            b['xpath'] = 'vid:storyboard/storyboarditem[{0}]/content'.format(idx + 1)
            b['uid'] = document['uid']
            b['path'] = document['path']
            blobs.append(b)
    if 'vid:transcodedVideos' in document['properties']:
        for idx, blob in enumerate(document['properties']['vid:transcodedVideos']):
            b = blob['content']
            b['xpath'] = 'vid:storyboard/transcodedVideoItem[{0}]/content'.format(idx + 1)
            b['uid'] = document['uid']
            b['path'] = document['path']
            blobs.append(b)
    if 'auxiliary_files:file' in document['properties']:
        for idx, blob in enumerate(document['properties']['auxiliary_files:file']):
            b = blob
            b['xpath'] = 'auxiliary_files:file/item[{0}]'.format(idx + 1)
            b['uid'] = document['uid']
            b['path'] = document['path']
            blobs.append(b)
    return blobs


def sizeof_fmt(num, suffix='B'):
    # http://stackoverflow.com/a/1094933
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)


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
