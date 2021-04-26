#!/bin/bash

if [ $# -eq 0 ]; then
    echo "No MonetDB directory instalation provided"
    exit 1
fi

echo 'Compiling native'
cd native
mvn clean install -DMonetDB_dir=$1 -P mac-release

#REALLY BAD CODE BELOW, REMOVE
echo 'Changing rpath for dependencies'
install_name_tool -change $1/lib/libmonetdbe.1.dylib @rpath/libmonetdbe.1.dylib target/libmonetdbe-java.dylib
otool -l target/libmonetdbe-java.dylib

echo 'Copying dependency libs to temp folder'
mkdir $1/lib-major/
cp $1/lib/libbat.??.dylib $1/lib/libmapi.??.dylib $1/lib/libmonetdb5.??.dylib $1/lib/libmonetdbe.?.dylib $1/lib/libmonetdbsql.??.dylib $1/lib/libstream.??.dylib $1/lib-major/

echo 'Changing rpath for dependencies'
cd $1/lib-major/
for file in *; do install_name_tool -delete_rpath $1/lib $file; done
for file in *; do install_name_tool -delete_rpath $1/lib/monetdb5 $file; done
for file in *; do install_name_tool -add_rpath @loader_path/. $file; done
cd -

echo 'Compiling jar'
cd ../java
mvn clean install -DMonetDB_dir=$1 -P mac-slim

#REALLY BAD CODE BELOW, REMOVE
rm -rf $1/lib-major/
