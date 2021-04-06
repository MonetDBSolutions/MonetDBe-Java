#!/usr/bin/env bash

# Go to main dir
cd ../
# Prepare directory for upload
mkdir -p upload/javadocs
# Copy header file
cp other/HEADER.html upload/
# Copy javadocs
cp -r java/target/apidocs/* upload/javadocs/
# Copy jar (change version number when necessary)
cp java/target/monetdbe-java-1.0-SNAPSHOT.jar upload/

# TODO
# Rsync the library files to the monet.org machine
#rsync -qz --ignore-times upload/* bernardo@monetdb.org:/var/www/html/downloads/MonetDBe-Java/
# Remove it in the end
#rm -rf upload