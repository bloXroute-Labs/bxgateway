#!/usr/bin/env bash

mkdir -p .venv
virtualenv -p python2 .venv
. .venv/bin/activate
pip install -r requirements.txt
pip install -r ../bxcommon/requirements.txt
echo ""
echo ""
echo ""
echo "**********UNIT TEST***********"
cd test/unit
PYTHONPATH=../../../bxcommon/src:../../src python -m unittest discover --verbose

echo ""
echo ""
echo ""
echo "**********INTEGRATION TEST***********"
cd ../integration
PYTHONPATH=../../../bxcommon/src:../../src python -m unittest discover --verbose

deactivate
