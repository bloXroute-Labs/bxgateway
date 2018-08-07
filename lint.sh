#!/usr/bin/env bash

mkdir -p .venv
virtualenv .venv
. .venv/bin/activate
pip install -r requirements.txt
echo ""
echo ""
echo ""
echo "**********PYLINT***********"
PYTHONPATH=../bxcommon/src/ pylint src/bxgateway --msg-template="{path}:{line}: [{msg_id}({symbol}), {obj}] {msg}" --rcfile=../bxcommon/pylintrc
deactivate