#!/bin/bash

if [ $# -eq 0 ]; then
    echo "No MonetDB directory instalation provided"
    exit 1
fi

cd native
mvn clean install -DMonetDB_dir=$1 -P mac-release

#REALLY BAD CODE BELOW, REMOVE
mkdir $1/lib-major/
cp $1/lib/libbat.??.dylib $1/lib/libmapi.??.dylib $1/lib/libmonetdb5.??.dylib $1/lib/libmonetdbe.?.dylib $1/lib/libmonetdbsql.??.dylib $1/lib/libstream.??.dylib $1/lib-major/

cd ../java
mvn clean deploy -DMonetDB_dir=$1 -P mac-slim,release

#REALLY BAD CODE BELOW, REMOVE
rm -rf $1/lib-major/
