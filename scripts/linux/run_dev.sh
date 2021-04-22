#!/bin/bash

rm -rf /tmp/lib*
javac -cp java/target/monetdbe-java-1.0-SNAPSHOT-linux.jar example/SimpleTypes.java 
java -cp java/target/monetdbe-java-1.0-SNAPSHOT-linux.jar:example/ SimpleTypes
