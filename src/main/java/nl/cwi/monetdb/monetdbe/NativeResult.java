package nl.cwi.monetdb.monetdbe;

import java.nio.ByteBuffer;

public class NativeResult {
    private ByteBuffer resultSet;
    private int affectedRows;

    public NativeResult(ByteBuffer resultSet, int affectedRows) {
        this.resultSet = resultSet;
        this.affectedRows = affectedRows;
    }

    public ByteBuffer getResultSet() {
        return resultSet;
    }

    public int getAffectedRows() {
        return affectedRows;
    }
}
