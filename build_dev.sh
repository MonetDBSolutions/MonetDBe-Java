#!/bin/bash

if [ $# -eq 0 ]; then
    echo "No MonetDB installation directory provided"
    exit 1
fi

if [ $# -eq 2 ]; then
    if [ "$2" == "true" ] || [ "$2" == "false" ]; then
        skipTests="-DskipTests=${2}"
    else
	echo "The second argument (whether to skip tests) must be true or false"
    fi
fi

echo "Building native library"
cd native
mvn clean install -DMonetDB_dir=$1 --no-transfer-progress
echo "Building dev jar"
cd ../java
mvn clean install $skipTests --no-transfer-progress
cd ..
echo "Done!"

#Clean up local test db
if [ -n "$skipTests" ] && [ "$2" == "false" ]; then
    rm -rf ../testdata/localdb/*
fi
