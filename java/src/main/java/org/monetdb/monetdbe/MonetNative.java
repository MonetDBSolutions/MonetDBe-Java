package org.monetdb.monetdbe;

import java.io.IOException;
import java.math.BigInteger;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

public class MonetNative {
    static {
        try {
            String os_name = System.getProperty("os.name").toLowerCase().trim();
            String filename = "/libmonetdbe-lowlevel.so";

            if (os_name.startsWith("linux")) {
                filename = "/libmonetdbe-lowlevel-Linux.so";
            }
            else if (os_name.startsWith("mac")) {
                filename = "/libmonetdbe-lowlevel-Mac OS X.so";
            }
            else if (os_name.startsWith("windows")) {
                //TODO Check name
                filename = "/libmonetdbe-lowlevel-windows.so";
            }

            Path temp_lib = Files.createTempFile("libmonetdbe-lowlevel",".so");
            URL is = MonetNative.class.getResource(filename);
            if (is == null) {
                throw new IOException("JNI library could not be found.");
            }
            Files.copy(is.openStream(), temp_lib, StandardCopyOption.REPLACE_EXISTING);
            System.load(temp_lib.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
        //System.loadLibrary("monetdbe-lowlevel");
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
