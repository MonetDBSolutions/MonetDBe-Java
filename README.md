# MonetDBe-Java
**A serverless and embedded MonetDB, now in Java!**

After the release of MonetDB/e Python, which brought the power of MonetDB data analytics to the world of Python embedded databases, we set out to expand its reach to the Java environment. Our goal is to provide a lightweight, full-featured embedded database that harnesses the performance of MonetDB’s columnar analytics while keeping the familiar JDBC interface. The power of a full-fledged database server at your fingertips as an embeddable library. The driver has been developed and tested for Linux, Mac and Windows.

MonetDB/e Java uses the core functionalities of our [embedded solution](https://www.monetdb.org/index.php/blog/MonetDBe-a-mature-embedded-SQL-DBMS) to implement the [JDBC 4.3 API](https://docs.oracle.com/javase/9/docs/api/java/sql/package-summary.html), resulting in a powerful and easy-to-use library. Using the JDBC interface makes migration from the older, legacy systems easier and allows developers to get hands-on experience with the capabilities of MonetDB/e quickly. While the API is not implemented in its entirety yet, all the main features from the Python and C versions are available. The full documentation can be found **here (link)**.

If you desire a driver with all the JDBC features and all the functionalities of the full MonetDB, you can check out [our JDBC driver](https://www.monetdb.org/Documentation/SQLreference/Programming/JDBC). But if you're looking for a lighter and faster version with only the core functionalities, you're in the right place! You can find the limitations of the embedded version below.

# Installation
There are several ways for you to get MonetDBe-Java on your system: 
- install it through maven
- download the jar from **our downloads page (link)** 
- or you can build the driver yourself (instructions and dependencies below)

## C Libraries (JNI)
MonetDBe-Java uses the MonetDBe C library through JNI, which means that it uses libraries which are OS-specific. Our goal is to provide a lightweight driver, so you can find different version for Linux, Mac and Windows. **TODO Cross platform jar?**

You can also find different versions of the Linux and Mac versions: 
- if you want a lighter driver, the **slim jar** is your choice, as it only features the MonetDB libraries. This means that you'll have to have the MonetDB dependencies installed in your system (you can find them below)
- if you want a portable version with every dependency, the **fat jar** is your best bet.

The Windows version features all the dependencies.

## Installing from Maven
**Not available on Maven central until release (TODO delete this)** You can find MonetDBe-Java in the Maven central repository, where you can choose the version that best suits you.

Just change the *\<classifier\>* tag on the maven dependency to get the different versions (OS and slim/fat jars).

**TODO remove the snapshot part when we release (this is still necessary to do until then)**
To try out the maven snapshot releases, please use the Sonatype snapshot repository:
```
<repositories>
    <repository>
        <id>Sonatype Snapshot</id>
        <url>https://oss.sonatype.org/content/repositories/snapshots</url>
    </repository>
</repositories>
```
**TODO Change the version of the maven dependencies when we release**
### Windows jar
```
<dependency>
  <groupId>monetdb</groupId>
  <artifactId>monetdbe-java</artifactId>
  <version>1.0-SNAPSHOT</version>
  <classifier>windows</classifier>
</dependency>
```
### Linux slim jar (only MonetDB libs included)
You need to have MonetDB's dependencies installed to use the Slim Jar.
```
<dependency>
  <groupId>monetdb</groupId>
  <artifactId>monetdbe-java</artifactId>
  <version>1.0-SNAPSHOT</version>
  <classifier>linux-slim</classifier>
</dependency>
```
#### Dependencies for the slim jar (Linux)
libpcre, libz

### Linux fat jar (all dependencies included)
```
<dependency>
  <groupId>monetdb</groupId>
  <artifactId>monetdbe-java</artifactId>
  <version>1.0-SNAPSHOT</version>
  <classifier>linux-fat</classifier>
</dependency>
```
### Mac Slim Jar (only MonetDB libs included)
You need to have MonetDB's dependencies installed to use the Slim Jar.
```
<dependency>
  <groupId>monetdb</groupId>
  <artifactId>monetdbe-java</artifactId>
  <version>1.0-SNAPSHOT</version>
  <classifier>mac-slim</classifier>
</dependency>
```
#### Dependencies for the Slim Jar (Mac)
libcrypto (OpenSSL), libpcre, libz, libxml2, libiconv, liblz4, liblzma, libcurl, libbz2
**TODO Is there a way to get these through brew?**

### Mac Fat Jar (all dependencies included)
```
<dependency>
  <groupId>monetdb</groupId>
  <artifactId>monetdbe-java</artifactId>
  <version>1.0-SNAPSHOT</version>
  <classifier>mac-fat</classifier>
</dependency>
```

## Installing MonetDBe-Java from source (Linux/Mac only)
### Dependencies
- The *JAVA_HOME* environmental variable must be set to your Java installation (JDK 8+ required)
- You must have a MonetDB installation
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
You can find the jar file in your **local maven repo** or in the **java/target/** directory (*monetdbe-java-1.0-SNAPSHOT-linux.jar* / *monetdbe-java-1.0-SNAPSHOT-mac.jar*)

### Script install
You can also use scripts for quickly building MonetDBe-Java on Mac and Linux.
It should be executed from the root of the repository.
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

# Usage

**TODO Maybe add some example code?**

## Extra features
MonetDBe-Java extends the JDBC specification, by allowing the use of BigInteger objects for integer values up to 128 bits to be retrieved from Result Sets (not available in the Windows version).

## Limitations
The following JDBC functionalities are not currently supported:
- setBigDecimal() and setBigInteger() in Prepared Statements
- Multithreaded access to connections and connection pooling
- Returning multiple Result Sets from a query
- Updating Result Sets
- Retrieving auto-generated keys
- Savepoints
- Array, SQLXML, Struct, NClob, RowId and Ref types
- OUT and INOUT parameters in Callable Statements
- The current clearParameters() implementation in Prepared Statements cleans up the whole Prepared Statement, not only the parameters

Some of these features are being worked on and are planned for further releases.
