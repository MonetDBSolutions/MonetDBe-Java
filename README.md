# MonetDB/e Java
**A serverless and embedded MonetDB, now in Java!**

After the release of MonetDB/e Python, which brought the power of MonetDB data analytics to the world of Python embedded databases, we set out to expand its reach to the Java environment. Our goal is to provide a lightweight, full-featured embedded database that harnesses the performance of MonetDB’s columnar analytics while keeping the familiar JDBC interface. The power of a full-fledged database server at your fingertips as an embeddable library. The driver has been developed and tested for Linux, Mac and Windows.

MonetDB/e Java uses the core functionalities of our [embedded solution](https://www.monetdb.org/index.php/blog/MonetDBe-a-mature-embedded-SQL-DBMS) to implement the [JDBC 4.3 API](https://docs.oracle.com/javase/9/docs/api/java/sql/package-summary.html), resulting in a powerful and easy-to-use library. Using the JDBC interface makes migration from the older, legacy systems easier and allows developers to get hands-on experience with the capabilities of MonetDB/e quickly. While the API is not implemented in its entirety yet, all the main features from the Python and C versions are available.

If you desire a driver with all the JDBC features and all the functionalities of the full MonetDB, you can check out [our JDBC driver](https://www.monetdb.org/Documentation/SQLreference/Programming/JDBC). But if you're looking for a lighter and faster version with only the core functionalities, you're in the right place! You can find the limitations of the embedded version below.

Documentation: [MonetDB/e Documentation](https://www.monetdb.org/downloads/MonetDBe-Java/javadocs/)

Jar downloads page: [Download MonetDB/e Java jars](https://www.monetdb.org/downloads/MonetDBe-Java/)

# Installation
There are several ways for you to get MonetDB/e Java on your system: 
- download the jar from [our downloads page](https://www.monetdb.org/downloads/MonetDBe-Java/)
- install it through maven
- build the driver yourself (instructions and dependencies below)
**Note**: Only the cross-platform jar is available through maven. All other release types can be found on [our website](https://www.monetdb.org/downloads/MonetDBe-Java/).

## Driver versions
MonetDB/e Java uses the MonetDB/e C library through JNI, which means that it uses libraries which are OS-specific. Our goal is to provide a lightweight driver, so you will find different version for Linux, Mac and Windows. For convenience, we also provide a **cross-platform jar** which works for the three operating systems.

You can also find different versions of the Linux and Mac driver: 
- if you want a lighter driver, the **slim jar** is your choice, as it only contains MonetDB libraries. This means that you'll have to have the MonetDB dependencies installed in your system (you can find them below)
- if you want a portable version with every dependency, the **fat jar** is your best bet.

The Windows version includes all the dependencies.

## Installing from Maven
You can find the cross-platform version of MonetDB/e Java in the Maven central repository. This jar works for all three supported OS, and contains every dependency. 

The cross-platform jar is a larger file than the OS-specific jars, since it contains every library for the three supported OSes. If you want a smaller jar, please use the OS-specific ones found in [our website](https://www.monetdb.org/downloads/MonetDBe-Java/) or build it yourself (instructions below).
```
<dependency>
  <groupId>monetdb</groupId>
  <artifactId>monetdbe-java</artifactId>
  <version>1.10</version>
</dependency>
```

## Installing MonetDB/e Java from source (Linux/Mac only)
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
This will install MonetDB/e Java in the local maven repository.
You can find the jar file in your **local maven repo** or in the **java/target/** directory (*monetdbe-java-1.10.jar*)

### Script install
You can also use scripts for quickly building MonetDB/e Java on Mac and Linux.
The script should be executed from the root of the repository.
```
$ build_dev.sh /path/to/monetdb/installation
```

## Running an example
After installing, you can run one of the examples in the example/ directory.
Example for the code below: SimpleTypes.java
```
$ javac -cp java/target/monetdbe-java-1.10.jar example/SimpleTypes.java
$ java -cp java/target/monetdbe-java-1.10.jar:example/ SimpleTypes
```

You can also execute the *run_dev.sh* script to run an example, just by passing it the example class name.
```
$ run_dev.sh SimpleTypes
$ run_dev.sh HelloWorld
```

# Usage
To use the MonetDB/e Java driver, you just need to include the dependency in your maven pom.xml file or include the jar in your classpath.

There are three types of connections in MonetDB/e Java, with different syntax:
- **In-memory databases**: `jdbc:monetdb:memory:`
- **Persistent file databases**: `jdbc:monetdb:file:<db-path>` where \<db-path\> is your persistent database directory
- **Connection to remote database**: `mapi:monetdb:<host>[:<port>]/<database>`, where \<host\>, \<port\> and \<database\> is the info about the remote database you're connecting to

You can change the connection/database configurations both through the connection URL (as an URL query) or through the `Properties` object passed to the `DriverManager.getConnection()` method (more info can be found in the documentation for [MonetDriver](https://www.monetdb.org/downloads/MonetDBe-Java/javadocs/org/monetdb/monetdbe/MonetDriver.html)).
    
The following example shows how to connect to an in-memory database, insert some data and then query it:
```
import java.sql.*

try {
    //Connect to in-memory database
    Connection conn = DriverManager.getConnection("jdbc:monetdb:memory:",null);
    
    //Create table and insert values
    Statement s = conn.createStatement();
    s.executeUpdate("CREATE TABLE example (i INTEGER, s STRING);");
    s.executeUpdate("INSERT INTO example VALUES (19,'hello'), (17,'world');");

    //Query table
    ResultSet rs = s.executeQuery("SELECT * FROM example;");

    //Fetch results
    while (rs.next()) {
        //Get columns
        rs.getInt(1);
        rs.getString(2);
    }
   
    //Close connection
    conn.close();
} catch (SQLException e) {
    e.printStackTrace();
}
```

You can find more examples of how to use MonetDB/e Java in the [examples directory](https://github.com/MonetDBSolutions/MonetDBe-Java/tree/master/example). To find out more about how to use the driver, please visit [our documentation pages](https://www.monetdb.org/downloads/MonetDBe-Java/javadocs/).

## Extra features
MonetDB/e Java supports in-memory databases (with configurable memory footprint), persistent file databases and connection to other MonetDB instances through a remote connection.
MonetDB/e Java extends the JDBC specification, by allowing the use of BigInteger objects for integer values up to 128 bits to be retrieved from Result Sets (not available in the Windows version).

## Limitations
The following JDBC functionalities are not currently supported:
- Multithreaded access to connections and connection pooling
- setBigDecimal() and setBigInteger() in Prepared Statements
- The current clearParameters() implementation in Prepared Statements cleans up the whole Prepared Statement, not only the parameters
- Returning multiple Result Sets from a query
- Updating Result Sets
- Retrieving auto-generated keys
- Savepoints
- Array, SQLXML, Struct, NClob, RowId and Ref types
- OUT and INOUT parameters in Callable Statements

Some of these features are being worked on and are planned for further releases.
