package nl.cwi.monetdb.monetdbe;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.*;
import java.sql.*;
import java.util.Calendar;
import java.util.Map;

public class MonetResultSet extends MonetWrapper implements ResultSet {
    /** The parental Statement object */
    private final MonetStatement statement;
    /** The native monet_result pointer */
    private ByteBuffer nativeResult;
    /** The metadata object */
    private MonetResultSetMetaData metaData;
    /** The number of rows in this ResultSet */
    private final int tupleCount;
    /** The current position of the cursor for this ResultSet object */
    private int curRow;

    private MonetColumn[] columns;
    /** Name defined in monetdbe_result C struct */
    private String name;

    //TODO Check these values
    /** The resultSetType of this ResultSet (forward or scrollable) */
    private int resultSetType = ResultSet.TYPE_SCROLL_INSENSITIVE;
    /** The concurrency of this ResultSet (currently only read-only) */
    private int concurrency = ResultSet.CONCUR_READ_ONLY;
    /** The suggested direction of fetching data (implemented but not used) */
    private int fetchDirection = ResultSet.FETCH_FORWARD;


    /** The warnings for this ResultSet object */
    private SQLWarning warnings;
    /** whether the last read field (via some getXyz() method) was NULL */
    private boolean lastReadWasNull = true;
    /** to store the fetchsize set. */
    private int fetchSize;
    /** whether the ResultSet is closed */
    private boolean closed = false;

    public MonetResultSet(MonetStatement statement, ByteBuffer nativeResult, int nrows, int ncols, String name) {
        this.statement = statement;
        this.nativeResult = nativeResult;
        this.tupleCount = nrows;
        this.curRow = 0;
        this.columns = MonetNative.monetdbe_result_fetch_all(nativeResult,nrows,ncols);
        //TODO Should this be created when the resultSet is, or only in the getMetadata method?
        this.metaData = new MonetResultSetMetaData(columns,ncols);
        this.name = name;
    }

    //Get Object / Type Object
    @Override
    public Object getObject(int columnIndex) throws SQLException {
        checkNotClosed();
        int type = columns[columnIndex].getMonetdbeType();
        switch (type) {
            case 0:
                return getBoolean(columnIndex);
            case 1:
                return getShort(columnIndex);
            case 2:
                return getShort(columnIndex);
            case 3:
                return getInt(columnIndex);
            case 4:
                return getLong(columnIndex);
            case 5:
                //TODO HUGEINT
                return null;
            case 6:
                return getInt(columnIndex);
            case 7:
                return getFloat(columnIndex);
            case 8:
                return getDouble(columnIndex);
            case 9:
                return getString(columnIndex);
            case 10:
                //TODO BLOB
                return null;
            case 11:
                return getDate(columnIndex);
            case 12:
                return getTime(columnIndex);
            case 13:
                return getTimestamp(columnIndex);
            default:
                return null;
        }
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getObject(findColumn(columnLabel));
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        //TODO
        return null;
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        //TODO
        return null;
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        return null;
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return null;
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        checkNotClosed();
        //TODO Check this
        String[] names = ((MonetResultSetMetaData)this.getMetaData()).getNames();
        if (columnLabel != null) {
            final int array_size = names.length;
            for (int i = 0; i < array_size; i++) {
                if (columnLabel.equals(names[i]))
                    return i + 1;
            }
            /* if an exact match did not succeed try a case insensitive match */
            for (int i = 0; i < array_size; i++) {
                if (columnLabel.equalsIgnoreCase(names[i]))
                    return i + 1;
            }
        }
        throw new SQLException("No such column name: " + columnLabel, "M1M05");
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        checkNotClosed();
        try {
            String val = columns[columnIndex].getString(curRow-1);
            if (val == null) {
                lastReadWasNull = true;
                return null;
            }
            lastReadWasNull = false;
            return val;
        } catch (IndexOutOfBoundsException e) {
            throw new SQLException("columnIndex out of bounds");
        }
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        checkNotClosed();
        try {
            Boolean val = columns[columnIndex].getBoolean(curRow-1);
            if (val == null) {
                lastReadWasNull = true;
                return false;
            }
            lastReadWasNull = false;
            return val;
        } catch (IndexOutOfBoundsException e) {
            throw new SQLException("columnIndex out of bounds");
        }
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        checkNotClosed();
        try {
            byte val = columns[columnIndex].getByte(curRow-1);
            if (val == 0) {
                lastReadWasNull = true;
                return 0;
            }
            lastReadWasNull = false;
            return val;
        } catch (IndexOutOfBoundsException e) {
            throw new SQLException("columnIndex out of bounds");
        }
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        checkNotClosed();
        try {
            Short val = columns[columnIndex].getShort(curRow-1);
            if (val == null) {
                lastReadWasNull = true;
                return 0;
            }
            lastReadWasNull = false;
            return val;
        } catch (IndexOutOfBoundsException e) {
            throw new SQLException("columnIndex out of bounds");
        }
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        checkNotClosed();
        try {
            Integer val = columns[columnIndex].getInt(curRow-1);
            if (val == null) {
                lastReadWasNull = true;
                return 0;
            }
            lastReadWasNull = false;
            return val;
        } catch (IndexOutOfBoundsException e) {
            throw new SQLException("columnIndex out of bounds");
        }
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        checkNotClosed();
        try {
            Long val = columns[columnIndex].getLong(curRow-1);
            if (val == null) {
                lastReadWasNull = true;
                return 0;
            }
            lastReadWasNull = false;
            return val;
        } catch (IndexOutOfBoundsException e) {
            throw new SQLException("columnIndex out of bounds");
        }
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        checkNotClosed();
        try {
            Float val = columns[columnIndex].getFloat(curRow-1);
            if (val == null) {
                lastReadWasNull = true;
                return 0;
            }
            lastReadWasNull = false;
            return val;
        } catch (IndexOutOfBoundsException e) {
            throw new SQLException("columnIndex out of bounds");
        }
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        checkNotClosed();
        try {
            Double val = columns[columnIndex].getDouble(curRow-1);
            if (val == null) {
                lastReadWasNull = true;
                return 0;
            }
            lastReadWasNull = false;
            return val;
        } catch (IndexOutOfBoundsException e) {
            throw new SQLException("columnIndex out of bounds");
        }
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        //TODO BIG DECIMAL
        return null;
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        //TODO BIG DECIMAL
        return null;
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        //TODO GET BYTES
        return null;
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        checkNotClosed();
        try {
            String val = columns[columnIndex].getString(curRow-1);
            if (val == null) {
                lastReadWasNull = true;
                return null;
            }
            lastReadWasNull = false;
            return Date.valueOf(val);
        } catch (IndexOutOfBoundsException e) {
            throw new SQLException("columnIndex out of bounds");
        }
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        checkNotClosed();
        try {
            String val = columns[columnIndex].getString(curRow-1);
            if (val == null) {
                lastReadWasNull = true;
                return null;
            }
            lastReadWasNull = false;
            return Time.valueOf(val);
        } catch (IndexOutOfBoundsException e) {
            throw new SQLException("columnIndex out of bounds");
        }
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        checkNotClosed();
        try {
            String val = columns[columnIndex].getString(curRow-1);
            if (val == null) {
                lastReadWasNull = true;
                return null;
            }
            lastReadWasNull = false;
            return Timestamp.valueOf(val);
        } catch (IndexOutOfBoundsException e) {
            throw new SQLException("columnIndex out of bounds");
        }
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        //TODO CALENDAR
        return null;
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        //TODO CALENDAR
        return null;
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        //TODO CALENDAR
        return null;
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        //TODO CALENDAR
        return null;
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        //TODO CALENDAR
        return null;
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return null;
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        //TODO BLOB
        return null;
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        //TODO CLOB
        return null;
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        checkNotClosed();
        try {
            String val = columns[columnIndex].getString(curRow-1);
            if (val == null) {
                lastReadWasNull = true;
                return null;
            }
            lastReadWasNull = false;
            return new URL(val);
        } catch (IndexOutOfBoundsException e) {
            throw new SQLException("columnIndex out of bounds");
        } catch (MalformedURLException e) {
            throw new SQLException("column is not a valid URL");
        }
    }

    //Meta sets/gets
    private void checkNotClosed() throws SQLException {
        if (isClosed())
            throw new SQLException("ResultSet is closed", "M1M20");
    }

    @Override
    public boolean isClosed() throws SQLException {
        return this.closed;
    }

    @Override
    public void close() throws SQLException {
        this.closed = true;
        //TODO Check for errors
        MonetNative.monetdbe_result_cleanup(((MonetConnection)this.statement.getConnection()).getDbNative(),nativeResult);
        this.columns = null;
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return metaData;
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        checkNotClosed();
        if (fetchDirection == ResultSet.FETCH_FORWARD) {
            throw new SQLException("(Absolute) positioning not allowed on forward " +
                    " only result sets!", "M1M05");
        }

        if (row < 0) {
            row = tupleCount + row + 1;
        }

        if (row < 0) {
            curRow = 0;    // before first
            return false;
        }
        else if (row > tupleCount) {
            curRow = tupleCount + 1;    // after last
            return false;
        }
        curRow = row;

        return true;
    }

    @Override
    public boolean relative(final int rows) throws SQLException {
        return absolute(curRow + rows);
    }

    @Override
    public boolean next() throws SQLException {
        return relative(1);
    }

    @Override
    public boolean previous() throws SQLException {
        return relative(-1);
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        checkNotClosed();
        return curRow == 0;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        checkNotClosed();
        return curRow == tupleCount + 1;
    }

    @Override
    public boolean isFirst() throws SQLException {
        checkNotClosed();
        return curRow == 1;
    }

    @Override
    public boolean isLast() throws SQLException {
        checkNotClosed();
        return curRow == tupleCount;
    }

    @Override
    public void beforeFirst() throws SQLException {
        absolute(0);
    }

    @Override
    public void afterLast() throws SQLException {
        absolute(tupleCount + 1);
    }

    @Override
    public boolean first() throws SQLException {
        return absolute(1);
    }

    @Override
    public boolean last() throws SQLException {
        return absolute(tupleCount);
    }

    @Override
    public Statement getStatement() throws SQLException {
        return statement;
    }

    @Override
    public int getRow() throws SQLException {
        return curRow;
    }

    @Override
    public int getType() throws SQLException {
        return resultSetType;
    }

    @Override
    public int getConcurrency() throws SQLException {
        return concurrency;
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public boolean wasNull() throws SQLException {
        return lastReadWasNull;
    }

    @Override
    public int getHoldability() throws SQLException {
        //TODO HOLDABILITY
        return 0;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        this.fetchDirection = direction;
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return fetchDirection;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        this.fetchSize = rows;
    }

    @Override
    public int getFetchSize() throws SQLException {
        return fetchSize;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        //TODO WARNINGS
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        //TODO WARNINGS
        this.warnings = new SQLWarning();
    }

    @Override
    public String getCursorName() throws SQLException {
        return name;
    }

    //Other gets
    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        return null;
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("rowId");
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getArray");
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getRef");
    }


    //Column name gets
    @Override
    public String getString(String columnLabel) throws SQLException {
        return getString(findColumn(columnLabel));
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return getBoolean(findColumn(columnLabel));
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return getByte(findColumn(columnLabel));
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return getShort(findColumn(columnLabel));
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return getInt(findColumn(columnLabel));
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return getLong(findColumn(columnLabel));
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return getFloat(findColumn(columnLabel));
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return getDouble(findColumn(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return getBigDecimal(findColumn(columnLabel),scale);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return getBigDecimal(findColumn(columnLabel));
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return getBytes(findColumn(columnLabel));
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return getDate(findColumn(columnLabel));
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return getTime(findColumn(columnLabel));
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getTimestamp(findColumn(columnLabel));
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        return getRef(findColumn(columnLabel));
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        return getBlob(findColumn(columnLabel));
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        return getClob(findColumn(columnLabel));
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        return getArray(findColumn(columnLabel));
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        return getRowId(findColumn(columnLabel));
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        return getURL(findColumn(columnLabel));
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        return getNClob(findColumn(columnLabel));
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        return getSQLXML(findColumn(columnLabel));
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        return getNString(findColumn(columnLabel));
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        return getAsciiStream(findColumn(columnLabel));
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        return getUnicodeStream(findColumn(columnLabel));
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        return getBinaryStream(findColumn(columnLabel));
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        return getCharacterStream(findColumn(columnLabel));
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        return getNCharacterStream(findColumn(columnLabel));
    }

    //Update
    @Override
    public boolean rowInserted() throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateObject(int columnIndex, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateObject(String columnLabel, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateObject(int columnIndex, Object x, SQLType targetSqlType) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateObject(String columnLabel, Object x, SQLType targetSqlType) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }
    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void insertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void deleteRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void refreshRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }
}
