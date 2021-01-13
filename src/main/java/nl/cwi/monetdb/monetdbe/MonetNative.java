package nl.cwi.monetdb.monetdbe;

import java.nio.ByteBuffer;

public class MonetNative {
    static {
        //System.load("/home/bernardo/MonetDBe-Java/build/libmonetdbe-lowlevel.so");
        System.load("/Users/bernardo/Monet/MonetDBe-Java/build/libmonetdbe-lowlevel.so");
    }

    protected static native ByteBuffer monetdbe_open(String dbdir);
    protected static native ByteBuffer monetdbe_open(String dbdir, int sessiontimeout, int querytimeout, int memorylimit, int nr_threads);
    protected static native int monetdbe_close(ByteBuffer db);

    protected static native MonetResultSet monetdbe_query(ByteBuffer db, String sql, MonetStatement statement, boolean largeUpdate);
    protected static native MonetColumn[] monetdbe_result_fetch_all(ByteBuffer nativeResult, int nrows, int ncols);
    protected static native String monetdbe_result_cleanup(ByteBuffer db, ByteBuffer nativeResult);

    protected static native String monetdbe_error(ByteBuffer db);
    protected static native String monetdbe_set_autocommit(ByteBuffer db, int value);
    protected static native boolean monetdbe_get_autocommit(ByteBuffer db);

    protected static native ByteBuffer monetdbe_prepare(ByteBuffer db, String sql, MonetPreparedStatement statement);
    protected static native String monetdbe_bind(ByteBuffer stmt, Object data, int type, int param);
    protected static native String monetdbe_bind_date(ByteBuffer stmt, int param, int year, int month, int day);
    protected static native String monetdbe_bind_time(ByteBuffer stmt, int param, int hours, int minutes, int seconds, int ms);
    protected static native String monetdbe_bind_timestamp(ByteBuffer stmt, int param, int year, int month, int day, int hours, int minutes, int seconds, int ms);
    protected static native MonetResultSet monetdbe_execute(ByteBuffer stmt, MonetPreparedStatement statement, boolean largeUpdate);
    protected static native String monetdbe_cleanup_statement (ByteBuffer db, ByteBuffer stmt);
}
