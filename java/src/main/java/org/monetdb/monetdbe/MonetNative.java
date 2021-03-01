package org.monetdb.monetdbe;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

public class MonetNative {
    static {
        try {
            //Java doesn't allow to load the library from within the jar
            //It must be copied to a temporary file before loading
            String os_name = System.getProperty("os.name").toLowerCase().trim();
            String suffix = ".so";
            if (os_name.startsWith("linux")) {
                suffix = ".so";
            }
            else if (os_name.startsWith("mac")) {
                suffix = ".dylib";
            }
            else if (os_name.startsWith("windows")) {
                suffix = ".ddl";
            }

            final String[] libs = {"libstream","libbat","libmapi","libmonetdb5","libmonetdbsql","libmonetdbe.1","libmonetdbe"};
            for (String l : libs) {
                copyLib(l,suffix);
            }
            loadLib("libmonetdbe-lowlevel",suffix);
        } catch (IOException e) {
            e.printStackTrace();
            //Try to load through the java.library.path variable
            System.loadLibrary("monetdbe-lowlevel");
        }
    }

    static void copyLib(String libName, String suffix) throws IOException {
        InputStream is = MonetNative.class.getResourceAsStream("/lib/" + libName + suffix);
        if (is == null) {
            throw new IOException("Library could not be found.");
        }
        java.nio.file.Files.copy(is, new java.io.File(System.getProperty("java.io.tmpdir") + libName + suffix).toPath(), StandardCopyOption.REPLACE_EXISTING);
    }

    static void loadLib(String libName, String suffix) throws IOException {
        InputStream is = MonetNative.class.getResourceAsStream("/lib/" + libName + suffix);
        if (is == null) {
            throw new IOException("Library could not be found.");
        }
        /* URL is = MonetNative.class.getResource("/lib/" + libName + suffix);
        String libPath = is.getPath().substring(5).replace("!","");
        System.out.println("Loading: " + libPath);
        System.load(libPath);*/
        Path temp_lib = Files.createTempFile(libName,suffix);
        Files.copy(is, temp_lib, StandardCopyOption.REPLACE_EXISTING);
        System.out.println(temp_lib.toString());
        System.load(temp_lib.toString());
    }

    protected static native ByteBuffer monetdbe_open(String dbdir);
    protected static native ByteBuffer monetdbe_open(String dbdir, int sessiontimeout, int querytimeout, int memorylimit, int nr_threads);
    protected static native ByteBuffer monetdbe_open(String dbdir, int sessiontimeout, int querytimeout, int memorylimit, int nr_threads, String host, int port, String user, String password);
    protected static native int monetdbe_close(ByteBuffer db);

    protected static native MonetResultSet monetdbe_query(ByteBuffer db, String sql, MonetStatement statement, boolean largeUpdate, int maxrows);
    protected static native MonetColumn[] monetdbe_result_fetch_all(ByteBuffer nativeResult, int nrows, int ncols);
    protected static native String monetdbe_result_cleanup(ByteBuffer db, ByteBuffer nativeResult);

    protected static native String monetdbe_error(ByteBuffer db);
    protected static native String monetdbe_set_autocommit(ByteBuffer db, int value);
    protected static native boolean monetdbe_get_autocommit(ByteBuffer db);

    protected static native ByteBuffer monetdbe_prepare(ByteBuffer db, String sql, MonetPreparedStatement statement);
    protected static native MonetResultSet monetdbe_execute(ByteBuffer stmt, MonetPreparedStatement statement, boolean largeUpdate, int maxrows);
    protected static native String monetdbe_cleanup_statement (ByteBuffer db, ByteBuffer stmt);

    protected static native String monetdbe_bind_bool(ByteBuffer stmt, int param, boolean data);
    protected static native String monetdbe_bind_byte(ByteBuffer stmt, int param, byte data);
    protected static native String monetdbe_bind_short(ByteBuffer stmt, int param, short data);
    protected static native String monetdbe_bind_int(ByteBuffer stmt, int param, int data);
    protected static native String monetdbe_bind_long(ByteBuffer stmt, int param, long data);
    protected static native String monetdbe_bind_hugeint(ByteBuffer stmt, int param, BigInteger data);
    protected static native String monetdbe_bind_float(ByteBuffer stmt, int param, float data);
    protected static native String monetdbe_bind_double(ByteBuffer stmt, int param, double data);

    protected static native String monetdbe_bind_string(ByteBuffer stmt, int param, String data);
    protected static native String monetdbe_bind_blob(ByteBuffer stmt, int param, byte[] data, long size);
    protected static native String monetdbe_bind_date(ByteBuffer stmt, int param, int year, int month, int day);
    protected static native String monetdbe_bind_time(ByteBuffer stmt, int param, int hours, int minutes, int seconds, int ms);
    protected static native String monetdbe_bind_timestamp(ByteBuffer stmt, int param, int year, int month, int day, int hours, int minutes, int seconds, int ms);
    protected static native String monetdbe_bind_decimal(ByteBuffer stmt, Object data, int type, int scale, int param);

    protected static native String monetdbe_bind_null(ByteBuffer db, int type, ByteBuffer stmt, int param);
}
