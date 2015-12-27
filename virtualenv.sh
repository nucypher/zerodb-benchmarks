#!/bin/bash

if [ ! -d ".venv" ]
then
    virtualenv .venv
    source .venv/bin/activate
    pip install -r requirements.txt
    ln -s .venv/bin/activate .

    cd zerodb-benchmarks
    zerodb-manage init_db --username test --passphrase testpassword --absolute-path
else
    echo "All done already"
    echo "  source activate -- activate virtual environment"
    echo "  deactivate      -- leave virtual environment"
fi
