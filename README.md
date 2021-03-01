# MonetDBe-Java
**Serverless embedded MonetDB in Java**

# Installing MonetDBe-Java
```
git clone https://github.com/MonetDBSolutions/MonetDBe-Java MonetDBe-Java
cd MonetDBe-Java
```
The JAVA_HOME path must be set before building.
```
$ mvn install -DMonetDB_dir=/path/to/monetdb/installation
```
This will install MonetDBe-Java in the local maven repository.
You can find the jar file in the java/target/ directory (*monetdbe-java-src-1.0-SNAPSHOT.jar*)
