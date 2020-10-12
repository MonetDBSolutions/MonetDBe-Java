package nl.cwi.monetdb.monetdbe;

import java.nio.ByteBuffer;

public class NativeResult {
    private ByteBuffer resultSet;
    private int affectedRows;

    private int nrows;
    private int ncols;
    private String name;
    private int last_id;

    public NativeResult(ByteBuffer resultSet, int nrows, int ncols, String name, int last_id) {
        this.resultSet = resultSet;
        this.nrows = nrows;
        this.ncols = ncols;
        this.name = name;
        this.last_id = last_id;
    }

    public NativeResult(ByteBuffer resultSet, int affectedRows) {
        this.resultSet = resultSet;
        this.affectedRows = affectedRows;
    }

    public NativeResult(int affectedRows) {
        this.affectedRows = affectedRows;
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

    public String getName() {
        return name;
    }

    public int getLast_id() {
        return last_id;
    }
}
