#!/usr/bin/env bash

set -e
set -x

(cd $PYENV_ROOT && git pull) || echo "failed to update pyenv root -- will proceed anyway."
pyenv install 3.9.9 || echo "failed to install python 3.9.9"
pyenv virtualenv 3.9.9 eth-etl || echo "failed to set up virtualenv"
pyenv shell eth-etl
pip3 install -e '.[dev]'
