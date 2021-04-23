# MonetDBe-Java
**Serverless embedded MonetDB in Java**

## Installing MonetDBe-Java from source
### Dependencies
The JAVA_HOME path must be set before building and you must have a MonetDB installation.
```
$ git clone https://github.com/MonetDBSolutions/MonetDBe-Java MonetDBe-Java
$ cd MonetDBe-Java
```
### Manual install
```
$ cd native
$ mvn clean install -DMonetDB_dir=/path/to/monetdb/installation
$ cd ../java
$ mvn clean install
```
This will install MonetDBe-Java in the local maven repository.
You can find the jar file in the **java/target/** directory or in your **local maven repo** (*monetdbe-java-1.0-SNAPSHOT-linux.jar* / *monetdbe-java-1.0-SNAPSHOT-mac.jar* / *monetdbe-java-1.0-SNAPSHOT-windows.jar*)

### Script install (experimental)
You can also use scripts for quickly building MonetDBe-Java on Mac and Linux.
They should be executed from the repo root directory.
```
$ scripts/build_dev.sh /path/to/monetdb/installation
```

## Running an example
After install, you can run one of the examples in the example/ directory.
Example for the code below: SimpleTypes.java
The OS in the jar name can be: linux, mac or windows.
```
$ javac -cp java/target/monetdbe-java-1.0-SNAPSHOT-{OS}.jar example/SimpleTypes.java
$ java -cp java/target/monetdbe-java-1.0-SNAPSHOT-{OS}.jar:example/ SimpleTypes
```

Alternatively, you can execute a script to run an example, just by passing it the example class name.
```
$ scripts/linux/run_dev.sh SimpleTypes
$ scripts/linux/run_dev.sh HelloWorld

$ scripts/mac/run_dev.sh SimpleTypes
```
