#!/bin/bash

for i in `ls multi-dbs`
do
    dirname=""multi-dbs/$i""
    if [ -d "$dirname" ]
    then
        pushd "$dirname"
        echo "Starting for db at $dirname"
        echo "==============================="
        zerodb-server&
        sleep 5
        popd
        ./run.py "$@"
        killall zerodb-server
        echo
    fi
done
