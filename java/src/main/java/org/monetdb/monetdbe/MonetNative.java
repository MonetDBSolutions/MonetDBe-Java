package org.monetdb.monetdbe;

import java.io.*;
import java.math.BigInteger;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Interface for C native methods in MonetDBe-Java.
 * Loads the compiled monetdbe_lowlevel C library and the libraries on
 * which it depends (found in the lib/ directories of the jar).
 * Because Java can't read libraries from inside the JAR, they are copied to a temporary location before load.
 */
//TODO Clean up libraries in temporary location?
//TODO Improve dependencies are resolved, this method must be horrible
//TODO Turn off debug prints
public class MonetNative {
    static {
        try {
            System.out.println("Native C libraries loading info:");
            String os_name = System.getProperty("os.name").toLowerCase().trim();
            String loadLib = "libmonetdbe-java";
            String loadLibExtension = null;
            String directory = null;
            List<String> dependencyDirs = new ArrayList<String>(){{ add("direct"); add("transitive"); }};

            if (os_name.startsWith("linux")) {
                loadLibExtension = ".so";
                directory = "linux";
            } else if (os_name.startsWith("mac")) {
                loadLibExtension = ".dylib";
                directory = "mac";
            } else if (os_name.startsWith("windows")) {
                loadLibExtension = ".dll";
                directory = "windows";
            }

            if (loadLibExtension != null) {
                Map<String,List<String>> dependencyMap = listDependencies(directory,dependencyDirs);
                if (dependencyMap != null) {
                    if (!loadLibExtension.equals(".dll")) {
                        for (String dependencyType : dependencyMap.keySet()) {
                            for (String dependencyLib : dependencyMap.get(dependencyType)) {
                                //Copy direct and transitive dependencies
                                copyLib(directory + "/" + dependencyType,dependencyLib);
                            }
                        }
                    }
                    else {
                        //Windows requires that both transitive and direct dependencies be loaded, no Unix automatic loading
                        //They also need to be loaded in the correct order
                        String[] transitiveDependencies = new String[]{"iconv-2.dll","lzma.dll","zlib1.dll","libcurl.dll","bz2.dll","libcrypto-1_1-x64.dll","pcre.dll","libxml2.dll"};
                        for (String td : transitiveDependencies) {
                            loadLib("windows/transitive",td);
                        }
                        String[] directDependencies = new String[]{"stream.dll","bat.dll","mapi.dll","monetdb5.dll","monetdbsql.dll","monetdbe.dll"};
                        for (String td : directDependencies) {
                            loadLib("windows/direct",td);
                        }
                    }
                    if (dependencyMap.size() > 0)
                        System.out.println("Copied dependencies to " + System.getProperty("java.io.tmpdir"));
                }
                else {
                    throw new IOException("Library dependencies could not be found");
                }
                loadLib(directory,loadLib+loadLibExtension);
                System.out.println("End of loading C libraries\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //Returns map with direct and transitive dependencies for the OS, based on the current jar
    static Map<String,List<String>> listDependencies(String subdirectory, List<String> dependencyDirs) throws IOException {
        URI uri = null;
        Path libRoot = null;
        try {
            uri = MonetNative.class.getResource("/lib/").toURI();
        } catch (URISyntaxException | NullPointerException e) {
            e.printStackTrace();
            return null;
        }

        //Loading within jar
        if ("jar".equalsIgnoreCase(uri.getScheme())) {
            libRoot = FileSystems.newFileSystem(uri, Collections.emptyMap()).getPath("/lib/" + subdirectory);
            System.out.println("Loading dependencies from within jar: " + libRoot.toString());
        }
        //Loading from file (IDE execution and maven unit tests)
        else {
            libRoot = Paths.get(uri.getPath(),"/" + subdirectory);
            System.out.println("Loading dependencies from filesystem: " + libRoot.toString());
        }
        Map<String,List<String>> dependencies = Files.walk(libRoot, 2)
                .collect(Collectors.groupingBy((path -> path.getParent().getFileName().toString()),
                        Collectors.mapping(
                                (paths -> paths.getFileName().toString()),
                                Collectors.toList())));
        dependencies.keySet().retainAll(dependencyDirs);
        return dependencies;
    }

    /**
     * Copy libraries to temporary location, to be in the rpath of libmonetdbe-lowlevel
     * @param directory Directory to copy from. Each OS has its own directory (linux, mac, windows)
     * @param libName Full library name to copy to temporary location
     * @throws IOException If the library could not be found
     */
    static void copyLib(String directory, String libName) throws IOException {
        System.out.println("Copying: " + libName);
        InputStream is = MonetNative.class.getResourceAsStream("/lib/" + directory + "/" + libName);
        if (is == null) {
            throw new IOException("Library " + libName +  " in /lib/" + directory + "/ could not be found.");
        }
        Files.copy(is, new java.io.File(System.getProperty("java.io.tmpdir") + "/" + libName).toPath(), StandardCopyOption.REPLACE_EXISTING);
    }

    /**
     * Copy library to temporary location, as Java cannot load it from within the jar
     * @param directory Directory to copy from. Each OS has its own directory (linux, mac, windows)
     * @param libName Full library name to load with System.load()
     * @throws IOException If the library could not be found
     */
    static void loadLib(String directory, String libName) throws IOException {
        System.out.println("Loading: " + libName);
        InputStream is = MonetNative.class.getResourceAsStream("/lib/" + directory + "/" + libName);
        if (is == null) {
            throw new IOException("Library " + libName +  " could not be found.");
        }
        Path temp_lib = new java.io.File(System.getProperty("java.io.tmpdir") + "/" +  libName).toPath();
        Files.copy(is, temp_lib, StandardCopyOption.REPLACE_EXISTING);
        System.load(temp_lib.toString());
    }

    /**
     * Open connection to memory or local directory database with default options.
     *
     * @param dbdir Directory for database, NULL for memory DBs
     * @param conn Parent Connection, needed for setting the created connection
     * @return Error message
     */
    protected static native String monetdbe_open(String dbdir, MonetConnection conn);

    /**
     * Open connection to memory or local directory database.
     *
     * @param dbdir Directory for database, NULL for memory DBs
     * @param conn Parent Connection, needed for setting the created connection
     * @param sessiontimeout Option for monetdbe_open() library function
     * @param querytimeout Option for monetdbe_open() library function
     * @param memorylimit Option for monetdbe_open() library function
     * @param nr_threads Option for monetdbe_open() library function
     * @return Error message
     */
    protected static native String monetdbe_open(String dbdir, MonetConnection conn, int sessiontimeout, int querytimeout, int memorylimit, int nr_threads);

    /**
     * Open connection to remote connection database.
     *
     * @param dbdir Directory for database, NULL for memory DBs
     * @param conn Parent Connection, needed for setting the created connection
     * @param sessiontimeout Option for monetdbe_open() library function
     * @param querytimeout Option for monetdbe_open() library function
     * @param memorylimit Option for monetdbe_open() library function
     * @param nr_threads Option for monetdbe_open() library function
     * @param host Remote host to connect to
     * @param port Remote port to connect to
     * @param database Remote database
     * @param user Username in the remote database
     * @param password Password in the remote database
     * @return Error message
     */
    protected static native String monetdbe_open(String dbdir, MonetConnection conn, int sessiontimeout, int querytimeout, int memorylimit, int nr_threads, String host, int port, String database, String user, String password);

    /**
     * Close the database connection.
     *
     * @param db C pointer to database
     * @return Error message
     */
    protected static native String monetdbe_close(ByteBuffer db);

    /**
     * Executes an SQL statement and returns either one result set for DQL queries,
     * an update count for DML queries or Statement.SUCCESS_NO_INFO (-2) for DDL queries.
     * The Java result (result set or update count) is set within this function.
     *
     * Currently, if the query can return multiple results, only the first one is actually returned.
     *
     * @param db C pointer to database
     * @param sql Query to execute
     * @param statement Parent Statement, needed for setting result set/update count
     * @param largeUpdate If this function was called from an executeLarge method
     * @param maxrows Maximum amount of rows to be returned in case a result set is returned
     * @return Error message
     */
    protected static native String monetdbe_query(ByteBuffer db, String sql, MonetStatement statement, boolean largeUpdate, int maxrows);

    /**
     * Retrieve result from monetdbe_result pointer to MonetColumn, the Java representation of the result columns.
     * Static length types are returned as a ByteBuffer, while variable length types are returned as Object[].
     * The constructor for MonetColumn is called within this function.
     *
     * @param nativeResult C pointer to result
     * @param nrows Number of rows in result
     * @param ncols Number of columns in result
     * @return Java object representation of result columns
     */
    protected static native MonetColumn[] monetdbe_result_fetch_all(ByteBuffer nativeResult, int nrows, int ncols);

    /**
     * Cleans up and closes a result set.
     *
     * @param db C pointer to database
     * @param nativeResult C pointer to result
     * @return Error message
     */
    protected static native String monetdbe_result_cleanup(ByteBuffer db, ByteBuffer nativeResult);

    /**
     * Returns latest error message from the database.
     *
     * @param db C pointer to database
     * @return Latest error message
     */
    protected static native String monetdbe_error(ByteBuffer db);

    /**
     * Sets auto-commit mode.
     *
     * @param db C pointer to database
     * @param value true or false
     * @return Error message
     */
    protected static native String monetdbe_set_autocommit(ByteBuffer db, int value);

    /**
     * Gets the current auto-commit value (either true or false).
     *
     * @param db C pointer to database
     * @return true if auto-commit mode is on, false otherwise
     */
    protected static native boolean monetdbe_get_autocommit(ByteBuffer db);

    /**
     * Prepares a reusable statement with configurable parameters.
     * The nParams (number of parameters), monetdbeTypes (array of types of the configurable parameters)
     * and statementNative (C pointer to prepared statement for bind and execution) variables
     * of the PreparedStatement object are set within this function.
     *
     * @param db C pointer to database
     * @param sql Statement to prepare
     * @param statement Parent PreparedStatement, needed for setting variables within function
     * @return Error message
     */
    protected static native String monetdbe_prepare(ByteBuffer db, String sql, MonetPreparedStatement statement);

    /**
     * Executes prepared statement (which was previously prepared and had its parameters bound) and returns result set
     * or update count, similarly to monetdbe_query function.
     *
     * @param stmt C pointer to prepared statement
     * @param statement Parent PreparedStatement, needed for setting result set/update count
     * @param largeUpdate If this function was called from an executeLarge method
     * @param maxrows Maximum amount of rows to be returned in case a result set is returned
     * @return Error message
     */
    protected static native String monetdbe_execute(ByteBuffer stmt, MonetPreparedStatement statement, boolean largeUpdate, int maxrows);

    /**
     * Cleans up and closes a previously prepared statement.
     *
     * @param db C pointer to database
     * @param stmt C pointer to prepared statement
     * @return Error message
     */
    protected static native String monetdbe_cleanup_statement(ByteBuffer db, ByteBuffer stmt);

    //TODO
    protected static native String monetdbe_clear_bindings(ByteBuffer db, ByteBuffer stmt);

    /**
     * Binds boolean parameter to prepared statement.
     *
     * @param stmt C pointer to prepared statement
     * @param param Parameter number
     * @param data Boolean value to bind
     * @return Error message
     */
    protected static native String monetdbe_bind_bool(ByteBuffer stmt, int param, boolean data);

    /**
     * Binds byte parameter to prepared statement.
     *
     * @param stmt C pointer to prepared statement
     * @param param Parameter number
     * @param data Byte value to bind
     * @return Error message
     */
    protected static native String monetdbe_bind_byte(ByteBuffer stmt, int param, byte data);

    /**
     * Binds short parameter to prepared statement.
     *
     * @param stmt C pointer to prepared statement
     * @param param Parameter number
     * @param data Short value to bind
     * @return Error message
     */
    protected static native String monetdbe_bind_short(ByteBuffer stmt, int param, short data);

    /**
     * Binds integer parameter to prepared statement.
     *
     * @param stmt C pointer to prepared statement
     * @param param Parameter number
     * @param data Integer value to bind
     * @return Error message
     */
    protected static native String monetdbe_bind_int(ByteBuffer stmt, int param, int data);

    /**
     * Binds long parameter to prepared statement.
     *
     * @param stmt C pointer to prepared statement
     * @param param Parameter number
     * @param data Long value to bind
     * @return Error message
     */
    protected static native String monetdbe_bind_long(ByteBuffer stmt, int param, long data);

    /**
     * Binds big integer parameter to prepared statement.
     *
     * Not working currently.
     *
     * @param stmt C pointer to prepared statement
     * @param param Parameter number
     * @param data Big integer value to bind
     * @return Error message
     */
    protected static native String monetdbe_bind_hugeint(ByteBuffer stmt, int param, BigInteger data);

    /**
     * Binds float parameter to prepared statement.
     *
     * @param stmt C pointer to prepared statement
     * @param param Parameter number
     * @param data Float value to bind
     * @return Error message
     */
    protected static native String monetdbe_bind_float(ByteBuffer stmt, int param, float data);

    /**
     * Binds double parameter to prepared statement.
     *
     * @param stmt C pointer to prepared statement
     * @param param Parameter number
     * @param data Double value to bind
     * @return Error message
     */
    protected static native String monetdbe_bind_double(ByteBuffer stmt, int param, double data);

    /**
     * Binds string parameter to prepared statement.
     *
     * @param stmt C pointer to prepared statement
     * @param param Parameter number
     * @param data String value to bind
     * @return Error message
     */
    protected static native String monetdbe_bind_string(ByteBuffer stmt, int param, String data);

    /**
     * Binds blob parameter to prepared statement.
     *
     * Not working currently.
     *
     * @param stmt C pointer to prepared statement
     * @param param Parameter number
     * @param data Blob value to bind, as byte array
     * @param size Size of byte array
     * @return Error message
     */
    protected static native String monetdbe_bind_blob(ByteBuffer stmt, int param, byte[] data, long size);

    /**
     * Binds date parameter to prepared statement.
     *
     * Not working currently.
     *
     * @param stmt C pointer to prepared statement
     * @param param Parameter number
     * @param year Date year
     * @param month Date month
     * @param day Date day
     * @return Error message
     */
    protected static native String monetdbe_bind_date(ByteBuffer stmt, int param, int year, int month, int day);

    /**
     * Binds time parameter to prepared statement.
     *
     * Not working currently.
     *
     * @param stmt C pointer to prepared statement
     * @param param Parameter number
     * @param hours Time hours
     * @param minutes Time minutes
     * @param seconds Time seconds
     * @param ms Time milliseconds
     * @return Error message
     */
    protected static native String monetdbe_bind_time(ByteBuffer stmt, int param, int hours, int minutes, int seconds, int ms);

    /**
     * Binds timestamp parameter to prepared statement.
     *
     * Not working currently.
     *
     * @param stmt C pointer to prepared statement
     * @param param Parameter number
     * @param year Timestamp year
     * @param month Timestamp month
     * @param day Timestamp day
     * @param hours Timestamp hours
     * @param minutes Timestamp minutes
     * @param seconds Timestamp seconds
     * @param ms Timestamp milliseconds
     * @return Error message
     */
    protected static native String monetdbe_bind_timestamp(ByteBuffer stmt, int param, int year, int month, int day, int hours, int minutes, int seconds, int ms);

    /**
     * Binds decimal parameter to prepared statement.
     *
     * Not working currently.
     *
     * @param stmt C pointer to prepared statement
     * @param data Unscaled value
     * @param type Type of unscaled value
     * @param scale Scale of the decimal parameter
     * @param param Parameter number
     * @return Error message
     */
    protected static native String monetdbe_bind_decimal(ByteBuffer stmt, Object data, int type, int scale, int param);

    /**
     * Binds a null value of any type to prepared statement.
     *
     * Not working for all types currently (datetime + decimal types).
     *
     * @param db C pointer to database
     * @param type Type of input parameter to set a null
     * @param stmt C pointer to prepared statement
     * @param param Parameter number
     * @return Error message
     */
    protected static native String monetdbe_bind_null(ByteBuffer db, int type, ByteBuffer stmt, int param);
}
