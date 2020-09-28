package nl.cwi.monetdb.monetdbe;

import java.nio.ByteBuffer;

public class MonetNative {
    static {
        System.load("libmonetdbe-lowlevel.so");
    }

    protected static native int monetdbe_open(ByteBuffer db, String url, ByteBuffer opts);
    protected static native int monetdbe_close(ByteBuffer db);
}
