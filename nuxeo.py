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
    parser.add_argument('path', nargs=1, help="root path")
    utils.get_common_options(parser)
    if argv is None:
        argv = parser.parse_args()

    nx = utils.Nuxeo(rcfile=argv.rcfile, loglevel=argv.loglevel.upper())

    campuses = nx.children(argv.path[0])

    summary_workbook = xlsxwriter.Workbook('summary.xlsx')
    summary_worksheet = summary_workbook.add_worksheet('summary')
    total_count = running_total = 0
    row = 1
    for campus in campuses:
        basename = os.path.basename(campus['path'])
        documents = nx.nxql(
            'select * from Document where ecm:path startswith"{0}"'.format(campus['path'])
        )
        (this_count, this_total) = forCampus(documents, basename)
        summary_worksheet.write(row, 0, basename)
        summary_worksheet.write(row, 1, this_total)
        summary_worksheet.write(row, 2, sizeof_fmt(this_total))
        summary_worksheet.write(row, 3, this_count))
        total_count = total_count + this_count
        running_total = running_total + this_total
        row = row + 1
    summary_worksheet.write(row, 1, running_total)
    summary_worksheet.write(row, 2, sizeof_fmt(running_total))
    summary_worksheet.write(row, 3, total_count)
    summary_workbook.close()


def forCampus(documents, basename):
    # open the workbook
    workbook = xlsxwriter.Workbook(u'{0}.xlsx'.format(basename))

    # set up a worksheet for each page

    header_format = workbook.add_format({'bold': True, })
    number_format = workbook.add_format()
    number_format.set_num_format('#,##0')
    
    campus = workbook.add_worksheet(basename)
    # headers
    campus.write(0, 0, 'row#', header_format)
    campus.write(0, 1, 'uid-nuxeo-document', header_format)
    campus.write(0, 2, 'path-nuxeo-document', header_format)
    campus.write(0, 3, 'xpath-nuxeo-document', header_format)
    campus.write(0, 4, 'filename', header_format)
    campus.write(0, 5, 'url', header_format)
    campus.write(0, 6, 'md5-digest', header_format)
    campus.write(0, 7, 'bytes', header_format)
    campus.write(0, 8, 'bytes-fmt', header_format)
    campus.write(0, 9, 'mime-type', header_format)
    # width
    campus.set_column(0, 0, 5, )
    campus.set_column(1, 1, 10, )
    campus.set_column(2, 2, 40, )
    campus.set_column(3, 3, 20, )
    campus.set_column(4, 4, 20, )
    campus.set_column(5, 5, 5, )
    campus.set_column(6, 6, 20, )
    campus.set_column(7, 7, 15, )
    campus.set_column(8, 8, 10, )

    row = 1
    running_total = 0
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
                campus.write(row, 8, sizeof_fmt(int(blob['length'])))
                campus.write(row, 9, blob['mime-type'],)
                row = row + 1
                running_total = running_total + int(blob['length'])
    campus.write(row, 7, running_total)
    campus.write(row, 8, sizeof_fmt(running_total))

    # note the current time
    run = '{}'.format(time.ctime())
    campus.write(row, 2, run)

    workbook.close()
    return (row - 1, running_total)


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
