#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import argparse
import os
from pynux import utils

from pprint import pprint as pp


def main(argv=None):

    parser = argparse.ArgumentParser(description='nxql via REST API')
    parser.add_argument('nxql', nargs=1, help="nxql query")
    parser.add_argument('--outdir', 
        help="directory to hold application/json+nxentity .json files")
    utils.get_common_options(parser)
    if argv is None:
        argv = parser.parse_args()

    nx = utils.Nuxeo(rcfile=argv.rcfile, loglevel=argv.loglevel.upper())

    documents = nx.nxql('select * from Document where ecm:path startswith"{0}"'.format(argv.nxql[0]))
    if argv.outdir:
        # Expand user- and relative-paths
        outdir = os.path.abspath(os.path.expanduser(argv.outdir))
        nx.copy_metadata_to_local(documents, outdir)
    else:
        for document in documents:
            for blob in blob_from_doc(document):
                if blob:
                    print(format_blob(blob).encode('utf-8'))


def format_blob(blob):
    return u'\t'.join([
        blob['uid'],
        blob['path'],
        blob['xpath'],
        blob['name'],
        blob['data'],
        u'{}:{}'.format(blob['digestAlgorithm'],blob['digest']),
        blob['length'],
        blob['mime-type'],
    ])
    

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
