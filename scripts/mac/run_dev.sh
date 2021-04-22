#!/bin/bash

rm -rf /var/folders/ps/_wtg4_610zg6d_8n2tp7yj540000gn/T/lib* 
javac -cp java/target/monetdbe-java-1.0-SNAPSHOT-mac.jar example/SimpleTypes.java
java -cp java/target/monetdbe-java-1.0-SNAPSHOT-mac.jar:example/ SimpleTypes
