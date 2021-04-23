#!/bin/bash

if [ $# -eq 0 ]; then
    echo "No MonetDB directory instalation provided"
    exit 1
fi

cd native
mvn clean install -DMonetDB_dir=$1 -P linux-release
cd ../java
mvn clean install -DMonetDB_dir=$1 -P linux-slim
