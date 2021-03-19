package org.monetdb.monetdbe;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.util.Collections;
import java.util.Iterator;
import java.util.stream.Stream;

/**
 * Interface for C native methods in MonetDBe-Java. Also loads the compiled monetdbe_lowlevel C library and the libraries on
 * which it depends (found in the lib/ directory of the jar).
 */
public class MonetNative {
    static {
        try {
            String os_name = System.getProperty("os.name").toLowerCase().trim();
            //Mac -> x86_64, Linux -> amd64
            String arch = System.getProperty("os.arch").toLowerCase().trim();

            String[] dependencyLibs = null;
            String loadLib = null;

            if (os_name.startsWith("linux")) {
                //dependencyLibs = new String[]{"libstream.so", "libbat.so", "libmapi.so", "libmonetdb5.so", "libmonetdbsql.so", "libmonetdbe.so"};
                dependencyLibs = new String[]{"libstream.so.14", "libbat.so.21", "libmapi.so.12", "libmonetdb5.so.30", "libmonetdbsql.so.11", "libmonetdbe.so.1"};
                //dependencyLibs = new String[]{"libstream.so.14.0.4","libbat.so.21.1.2","libmapi.so.12.0.6","libmonetdb5.so.30.0.5","libmonetdbsql.so.11.40.0","libmonetdbe.so.1.0.2"};
                loadLib = "libmonetdbe-lowlevel.so";
            } else if (os_name.startsWith("mac")) {
                //dependencyLibs = new String[]{"libstream.dylib", "libbat.dylib", "libmapi.dylib", "libmonetdb5.dylib", "libmonetdbsql.dylib", "libmonetdbe.dylib"};
                dependencyLibs = new String[]{"libstream.14.dylib", "libbat.21.dylib", "libmapi.12.dylib", "libmonetdb5.30.dylib", "libmonetdbsql.11.dylib", "libmonetdbe.1.dylib"};
                //dependencyLibs = new String[]{"libstream.14.0.4.dylib", "libbat.21.1.2.dylib", "libmapi.12.0.6.dylib", "libmonetdb5.30.0.5.dylib", "libmonetdbsql.11.40.0.dylib", "libmonetdbe.1.0.2.dylib"};
                loadLib = "libmonetdbe-lowlevel.dylib";
            } else if (os_name.startsWith("windows")) {
                dependencyLibs = new String[]{"stream.lib","bat.lib","mapi.lib","monetdb5.lib","monetdbsql.lib","monetdbe.lib"};
                loadLib = "libmonetdbe-lowlevel.dll";
            }

            if (dependencyLibs != null && loadLib != null) {
                for (String l : dependencyLibs) {
                    copyLib(l);
                }
                //copyAllLibs();
                //Java doesn't allow to load the library from within the jar
                //It must be copied to a temporary file before loading
                loadLib(loadLib);
            }
            else {
                //TODO: Error
            }
            //TODO Delete temp files?
        } catch (IOException e) {
            e.printStackTrace();
            //Try to load through the java.library.path variable
            //System.loadLibrary("monetdbe-lowlevel");
        }
    }


    static void copyAllLibs() throws IOException {
        URI uri = null;
        try {
            uri = MonetNative.class.getResource("/lib/").toURI();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        Path myPath;
        if (uri.getScheme().equals("jar")) {
            FileSystem fileSystem = FileSystems.newFileSystem(uri, Collections.<String, Object>emptyMap());
            myPath = fileSystem.getPath("/lib/");
        } else {
            myPath = Paths.get(uri);
        }
        Stream<Path> walk = Files.walk(myPath, 1);
        for (Iterator<Path> it = walk.iterator(); it.hasNext();){
            String s = it.next().toString();
            if (!s.equals("/lib/") && !s.equals("/lib")) {
                copyLib(s.substring(5));
            }
        }
    }

    /**
     * Copy libraries to temporary location, to be in the rpath of libmonetdbe-lowlevel
     * @param libName Full library name to copy to temporary location
     */
    static void copyLib(String libName) throws IOException {
        System.out.println("Copying: " + libName);
        InputStream is = MonetNative.class.getResourceAsStream("/lib/" + libName);
        if (is == null) {
            throw new IOException("Library " + libName +  " could not be found.");
        }
        Files.copy(is, new java.io.File(System.getProperty("java.io.tmpdir") + "/" + libName).toPath(), StandardCopyOption.REPLACE_EXISTING);
        System.out.println(System.getProperty("java.io.tmpdir") + "/" + libName);
    }

    /**
     * Copy library to temporary location, as Java cannot load it from within the jar
     * @param libName Full library name to load with System.load()
     */
    static void loadLib(String libName) throws IOException {
        //System.out.println("Loading: " + libName);
        InputStream is = MonetNative.class.getResourceAsStream("/lib/" + libName);
        if (is == null) {
            throw new IOException("Library " + libName +  " could not be found.");
        }
        Path temp_lib = Files.createTempFile(libName, "");
        Files.copy(is, temp_lib, StandardCopyOption.REPLACE_EXISTING);
        System.out.println(temp_lib.toString());
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
