#!/bin/bash

if [ $# -eq 0 ]; then
    echo "No MonetDB directory instalation provided"
    exit 1
fi

cd native
mvn clean install -DMonetDB_dir=$1 -P linux-release

#REALLY BAD CODE BELOW, REMOVE
mkdir $1/lib-major/
cp $1/lib64/libbat.so.?? $1/lib64/libmapi.so.?? $1/lib64/libmonetdb5.so.?? $1/lib64/libmonetdbe.so.? $1/lib64/libmonetdbsql.so.?? $1/lib64/libstream.so.?? $1/lib-major/

cd ../java
mvn clean install -DMonetDB_dir=$1 -P linux-slim

#REALLY BAD CODE BELOW, REMOVE
rm -rf $1/lib-major/