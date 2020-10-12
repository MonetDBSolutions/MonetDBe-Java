package nl.cwi.monetdb.monetdbe;

import java.nio.ByteBuffer;

public class NativeResult {
    private ByteBuffer resultSet;
    private int affectedRows;

    private int nrows;
    private int ncols;

    public NativeResult(ByteBuffer resultSet, int nrows, int ncols) {
        this.resultSet = resultSet;
        this.nrows = nrows;
        this.ncols = ncols;
        this.affectedRows = 0;
    }

    public NativeResult(ByteBuffer resultSet, int affectedRows) {
        this.resultSet = resultSet;
        this.affectedRows = affectedRows;
    }

    public NativeResult(int affectedRows) {
        this.affectedRows = affectedRows;
        this.resultSet = null;
    }

    public ByteBuffer getResultSet() {
        return resultSet;
    }

    public int getAffectedRows() {
        return affectedRows;
    }

    public int getNrows() {
        return nrows;
    }

    public int getNcols() {
        return ncols;
    }
}
