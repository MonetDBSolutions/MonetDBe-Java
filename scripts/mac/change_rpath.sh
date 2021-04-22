#!/bin/bash

if [ $# -eq 0 ]; then
    echo "No MonetDB directory instalation provided"
    exit 1
fi

cd $1
for file in ./*.dylib; do install_name_tool -delete_rpath $1 $file; done
for file in ./*.dylib; do install_name_tool -delete_rpath $1/monetdb5 $file; done
for file in ./*.dylib; do install_name_tool -add_rpath @loader_path/. $file; done
