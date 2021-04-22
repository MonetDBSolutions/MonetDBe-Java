#!/bin/bash

if [ $# -eq 0 ]; then
    echo "No MonetDB directory instalation provided"
    exit 1
fi

cd native
mvn clean install -DMonetDB_dir=$1
cd ../java
mvn clean install -Dbuild_type=slimjar -DMonetDB_dir=$1
