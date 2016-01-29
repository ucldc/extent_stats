#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function

import sys
import argparse
import os
from pynux import utils
import xlsxwriter
import time
import gzip
import datetime

from pprint import pprint as pp

today = datetime.date.today()

def main(argv=None):
    parser = argparse.ArgumentParser(description='extent stats via Nuxeo REST API')
    parser.add_argument('path', nargs=1, help="root path")
    parser.add_argument('outdir', nargs=1,)
    parser.add_argument('--no-s3-check', dest='s3_check', action='store_false')
    utils.get_common_options(parser)
    if argv is None:
        argv = parser.parse_args()

    # look up all the files in S3, so we can double check that all
    # the files exist as we loop through Nuxeo
    file_check = None
    pp(argv.s3_check)
    if argv.s3_check:
        from boto import s3
        from boto.s3.connection import OrdinaryCallingFormat
        file_check = {}
        conn = s3.connect_to_region('us-west-2', calling_format=OrdinaryCallingFormat())
        bucket = conn.get_bucket('data.nuxeo.cdlib.org.oregon')
        for count, key in enumerate(bucket.list()):
            file_check[key.name] = key.size
            if count % 50000 == 0:
                print('{0} s3 files memorized'.format(count), file=sys.stderr)

    nx = utils.Nuxeo(rcfile=argv.rcfile, loglevel=argv.loglevel.upper())

    campuses = nx.children(argv.path[0])

    summary_workbook = xlsxwriter.Workbook(os.path.join(argv.outdir[0],'{}-summary.xlsx'.format(today)))
    # cell formats
    header_format = summary_workbook.add_format({'bold': True, })
    number_format = summary_workbook.add_format()
    number_format.set_num_format('#,##0')

    summary_worksheet = summary_workbook.add_worksheet('summary')
    # headers
    summary_worksheet.write(0, 1, 'total files', header_format)
    summary_worksheet.write(0, 2, 'total bytes', header_format)
    summary_worksheet.write(0, 4, 'deduplicated files', header_format)
    summary_worksheet.write(0, 5, 'deduplicated bytes', header_format)
    # widths
    summary_worksheet.set_column(0, 6, 10, )
    true_count = dedup_total = total_count = running_total = 0
    row = 1
    for campus in campuses:
        basename = os.path.basename(campus['path'])
        documents = nx.nxql(
            'select * from Document where ecm:path startswith"{0}"'.format(campus['path'])
        )
        (this_count, this_total, dedup_count, dedup_bytes) = forCampus(documents, basename, file_check, argv.outdir[0])
        summary_worksheet.write(row, 0, basename)
        summary_worksheet.write(row, 1, this_count, number_format)
        summary_worksheet.write(row, 2, this_total, number_format)
        summary_worksheet.write(row, 3, sizeof_fmt(this_total))
        summary_worksheet.write(row, 4, dedup_count, number_format)
        summary_worksheet.write(row, 5, dedup_bytes, number_format)
        summary_worksheet.write(row, 6, sizeof_fmt(dedup_bytes))
        total_count = total_count + this_count  # number of files
        running_total = running_total + this_total  # number of bytes
        true_count = true_count + dedup_count
        dedup_total = dedup_total + dedup_bytes  # number of bytes
        row = row + 1
    summary_worksheet.write(row, 0, '{}'.format(today))
    summary_worksheet.write(row, 1, total_count, number_format)
    summary_worksheet.write(row, 2, running_total, number_format)
    summary_worksheet.write(row, 3, sizeof_fmt(running_total))
    summary_worksheet.write(row, 4, true_count, number_format)
    summary_worksheet.write(row, 5, dedup_total, number_format)
    summary_worksheet.write(row, 6, sizeof_fmt(dedup_total))
    summary_workbook.close()


def fileCheck(blob, file_check):
    s3_size = file_check.get(blob['digest'], None)
    if not s3_size:
        print('{0} from {1} {2} not found in S3'
              .format(blob['digest'], blob['path'], blob['xpath']))
    if file_dict.get(blob['digest'], 0) != int(blob['length']):
        print('{0} from {1} {2} s3 size {3} does not match nuxeo size {3}'
              .format(blob['digest'],
                      blob['path'],
                      blob['xpath'],
                      s3_size,
                      blob['length']))


def forCampus(documents, basename, file_check, outdir):
    deduplicate = {}
    row = 1
    running_total = 0
    campus = gzip.open(os.path.join(outdir, '{}-{}.txt.gz'.format(today, basename)), 'wb')
    for document in documents:
        for blob in blob_from_doc(document):
            if blob:
                if file_check:
                    check_file(blob, file_check)
                deduplicate[blob['digest']] = int(blob['length'])
                campus.write(u'\t'.join([
                    u'uid={}'.format(blob['uid']),
                    u'path={}'.format(blob['path']),
                    u'xpath={}'.format(blob['xpath']),
                    u'name={}'.format(blob['name']),
                    u'data={}'.format(blob['data']),
                    u'md5={}'.format(blob['digest']),
                    u'size={}'.format(int(blob['length'])),
                    u'size_h={}'.format(sizeof_fmt(int(blob['length']))),
                    u'media={}'.format(blob['mime-type']),
                ]))
                campus.write(u'\n')
                row = row + 1
                running_total = running_total + int(blob['length'])
    campus.close()
    return (row - 1, running_total, len(deduplicate), sum(deduplicate.itervalues()))


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
