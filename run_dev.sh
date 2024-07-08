#!/bin/bash
set -x

if [ $# -eq 0 ]; then
    echo "No example to run was provided"
    exit 1
fi

javac -cp java/target/monetdbe-java-1.0-SNAPSHOT.jar example/$1.java 
java -cp java/target/monetdbe-java-1.0-SNAPSHOT.jar:example/ $1
