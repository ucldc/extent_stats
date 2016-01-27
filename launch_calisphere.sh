#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )" # http://stackoverflow.com/questions/59895

cd $DIR

if [[ ! -e venv27 ]]; then
  virtualenv --python python2.7 venv27
  pip install xlsxwriter requests
fi

. ve/bin/activate
PYTHONWARNINGS="ignore:Unverified HTTPS request" ./calisphere.py /dsc/webdocs/voro/calisphere.org
