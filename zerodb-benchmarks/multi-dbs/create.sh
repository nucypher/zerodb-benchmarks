#!/bin/bash

CREATE_CMD=`readlink -f ../create.py`

for batch_size in 200 400 800 1200 2000 4000 8000 12000 20000
do
    DIRNAME="db-$batch_size"
    mkdir "$DIRNAME"
    pushd "$DIRNAME"
        echo "Creating $DIRNAME..."
        zerodb-manage init_db --username test --passphrase testpassword --absolute-path
        $CREATE_CMD --batch-size $batch_size --use-multiprocessing --db-dir `readlink -f .`
        echo
    popd
done
