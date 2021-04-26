#!/bin/bash

if [ $# -eq 0 ]; then
    echo "No MonetDB directory instalation provided"
    exit 1
fi

echo 'Compiling native'
cd native
mvn clean install -DMonetDB_dir=$1 -P linux-release

#REALLY BAD CODE BELOW, REMOVE
echo 'Copying libs'
mkdir $1/lib-major/
cp $1/lib/libbat.so.?? $1/lib/libmapi.so.?? $1/lib/libmonetdb5.so.?? $1/lib/libmonetdbe.so.? $1/lib/libmonetdbsql.so.?? $1/lib/libstream.so.?? $1/lib-major/

echo 'Changing rpath'
cd $1/lib-major/
for file in *; do chrpath -r '$ORIGIN/.' $file; done
cd -

cd ../java
mvn clean install -DMonetDB_dir=$1 -P linux-slim

#REALLY BAD CODE BELOW, REMOVE
rm -rf $1/lib-major/
