# MonetDBe-Java
**Serverless embedded MonetDB in Java**

## Installing MonetDBe-Java from Maven
To try out the maven snapshot releases, please use the Sonatype snapshot repository:
```
<repositories>
    <repository>
        <id>Sonatype Snapshot</id>
        <url>https://oss.sonatype.org/content/repositories/snapshots</url>
    </repository>
</repositories>
```
### Windows Jar
```
<dependency>
  <groupId>monetdb</groupId>
  <artifactId>monetdbe-java</artifactId>
  <version>1.0-SNAPSHOT</version>
  <classifier>windows</classifier>
</dependency>
```
### Linux Slim Jar (only MonetDB libs included)
You need to have MonetDB's dependencies installed to use the Slim Jar (check the list below).
```
<dependency>
  <groupId>monetdb</groupId>
  <artifactId>monetdbe-java</artifactId>
  <version>1.0-SNAPSHOT</version>
  <classifier>linux-slim</classifier>
</dependency>
```
### Mac Slim Jar (only MonetDB libs included)
You need to have MonetDB's dependencies installed to use the Slim Jar (check the list below).
```
<dependency>
  <groupId>monetdb</groupId>
  <artifactId>monetdbe-java</artifactId>
  <version>1.0-SNAPSHOT</version>
  <classifier>mac-slim</classifier>
</dependency>
```
### Mac Fat Jar (all dependencies included)
```
<dependency>
  <groupId>monetdb</groupId>
  <artifactId>monetdbe-java</artifactId>
  <version>1.0-SNAPSHOT</version>
  <classifier>mac-fat</classifier>
</dependency>
```
### Dependencies for the Slim Jar (Linux/Mac)
**TODO**
Temp list: libxml2, libpcre, OpenSSL

## Installing MonetDBe-Java from source (Linux/Mac only)
### Dependencies
- JAVA_HOME path must be set to your Java installation (JDK 8+ required)
- Must have a MonetDB installation
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

### Script install
You can also use scripts for quickly building MonetDBe-Java on Mac and Linux.
They should be executed from the repo root directory.
```
$ build_dev.sh /path/to/monetdb/installation
```

## Running an example
After installing, you can run one of the examples in the example/ directory.
Example for the code below: SimpleTypes.java
```
$ javac -cp java/target/monetdbe-java-1.0-SNAPSHOT.jar example/SimpleTypes.java
$ java -cp java/target/monetdbe-java-1.0-SNAPSHOT.jar:example/ SimpleTypes
```

You can also execute the *run_dev.sh* script to run an example, just by passing it the example class name.
```
$ run_dev.sh SimpleTypes
$ run_dev.sh HelloWorld
```
