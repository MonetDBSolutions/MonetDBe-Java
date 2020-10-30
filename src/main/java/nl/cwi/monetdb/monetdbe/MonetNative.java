package nl.cwi.monetdb.monetdbe;

import java.nio.ByteBuffer;

public class MonetNative {
    static {
        System.load("/home/bernardo/MonetDBe-Java/build/libmonetdbe-lowlevel.so");
    }

    protected static native ByteBuffer monetdbe_open(String dbdir);
    protected static native ByteBuffer monetdbe_open(String dbdir, int sessiontimeout, int querytimeout, int memorylimit, int nr_threads);
    protected static native int monetdbe_close(ByteBuffer db);
    protected static native MonetResultSet monetdbe_query(ByteBuffer db, String sql, MonetStatement statement);
    protected static native MonetColumn[] monetdbe_result_fetch_all(ByteBuffer nativeResult, int nrows, int ncols);
    protected static native String monetdbe_result_cleanup(ByteBuffer db, ByteBuffer nativeResult);
    protected static native String monetdbe_error(ByteBuffer db);
    protected static native String monetdbe_set_autocommit(ByteBuffer db, int value);
    protected static native boolean monetdbe_get_autocommit(ByteBuffer db);
}
