package org.monetdb.monetdbe;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.*;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.Calendar;
import java.util.Map;
import java.util.TimeZone;

public class MonetResultSet extends MonetWrapper implements ResultSet {
    private final MonetStatement statement;
    private ByteBuffer nativeResult;
    private MonetResultSetMetaData metaData;
    private final int tupleCount;
    private int curRow;
    private int columnCount;

    //Columns of fetched data
    private MonetColumn[] columns;
    private String name;

    //Ignored
    private int resultSetType = ResultSet.TYPE_SCROLL_INSENSITIVE;
    private int concurrency = ResultSet.CONCUR_READ_ONLY;
    private int fetchDirection = ResultSet.FETCH_UNKNOWN;
    private int resultSetHoldability = ResultSet.HOLD_CURSORS_OVER_COMMIT;
    private int fetchSize;

    private SQLWarning warnings;
    private boolean lastReadWasNull = true;
    private boolean closed = false;

    MonetResultSet(MonetStatement statement, ByteBuffer nativeResult, int nrows, int ncols, String name, int maxRows) {
        this.statement = statement;
        this.nativeResult = nativeResult;
        this.columnCount = ncols;
        this.curRow = 0;
        this.columns = MonetNative.monetdbe_result_fetch_all(nativeResult,nrows,ncols);

        //Failed fetch, destroy resultset
        if (this.columns == null) {
            System.out.println("ResultSet fetch error");
            try {
                this.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        this.name = name;

        if (maxRows != 0 && maxRows < nrows) {
            this.tupleCount = maxRows;
        }
        else {
            this.tupleCount = nrows;
        }
    }

    //Default Object type for a given SQL Type
    @Override
    public Object getObject(int columnIndex) throws SQLException {
        checkNotClosed();
        if (columnIndex > columnCount) {
            throw new SQLException("columnIndex is not valid");
        }
        int type = columns[columnIndex-1].getMonetdbeType();
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
                return getHugeInt(columnIndex);
            case 6:
                return getInt(columnIndex);
            case 7:
                return getFloat(columnIndex);
            case 8:
                return getDouble(columnIndex);
            case 9:
                return getString(columnIndex);
            case 10:
                return getBlob(columnIndex);
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
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        checkNotClosed();
        if (columnIndex > columnCount) {
            throw new SQLException("columnIndex is not valid");
        }
        else if (map == null) {
            //If there is no mapping, return default Java class
            return getObject(columnIndex);
        }

        int monetdbeType = columns[columnIndex-1].getMonetdbeType();
        String sqlDefaultType = MonetTypes.getSQLTypeNameFromMonet(monetdbeType);
        Class<?> convertClass;

        if (sqlDefaultType.equals("NULL")) {
            return null;
        }

        //Map contains mapping to Java class from default SQL type for the column's monetdbeType
        if (map.containsKey(sqlDefaultType)) {
            convertClass = map.get(sqlDefaultType);
            return getObject(columnIndex,convertClass);
        }
        //Alternative SQL types for the column's monetdbeType
        else {
            for (String keyType : map.keySet()) {
                if (MonetTypes.getMonetTypeIntFromSQLName(keyType) == monetdbeType) {
                    convertClass = map.get(keyType);
                    return getObject(columnIndex,convertClass);
                }
            }
        }
        //If there is no possible mapping between the monetdbetype and a SQL type in the argument map, return default Java class
        return getObject(columnIndex);
    }

    //This object conversion probably doesn't work well. We can't simply cast from the default Java object associated with the monetdbetype.
    //Wasn't implemented in the last version
    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        checkNotClosed();
        if (type == null) {
            throw new SQLException("Type is null");
        }
        else if (columnIndex > columnCount) {
            throw new SQLException("columnIndex is not valid");
        }

        int monetdbeType = columns[columnIndex-1].getMonetdbeType();
        if (MonetTypes.convertTojavaClass(monetdbeType,type)) {
            Object defaultValue = getObject(columnIndex);
            return type.cast(defaultValue);
        }
        else {
            throw new SQLException("Conversion is not supported");
        }
    }

    //Gets
    @Override
    public int findColumn(String columnLabel) throws SQLException {
        checkNotClosed();
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
        if (curRow <= 0 || curRow > tupleCount)
            throw new SQLException("Current row " + curRow + " does not support operation");
        try {
            String val = columns[columnIndex-1].getString(curRow-1);
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
        if (curRow <= 0 || curRow > tupleCount)
            throw new SQLException("Current row " + curRow + " does not support operation");
        try {
            Boolean val = columns[columnIndex-1].getBoolean(curRow-1);
            lastReadWasNull = false;
            return val;
        } catch (IndexOutOfBoundsException e) {
            throw new SQLException("columnIndex out of bounds");
        }
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        checkNotClosed();
        if (curRow <= 0 || curRow > tupleCount)
            throw new SQLException("Current row " + curRow + " does not support operation");
        try {
            Byte val = columns[columnIndex-1].getByte(curRow-1);
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
    public short getShort(int columnIndex) throws SQLException {
        checkNotClosed();
        if (curRow <= 0 || curRow > tupleCount)
            throw new SQLException("Current row " + curRow + " does not support operation");
        try {
            Short val = columns[columnIndex-1].getShort(curRow-1);
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
        if (curRow <= 0 || curRow > tupleCount)
            throw new SQLException("Current row " + curRow + " does not support operation");
        try {
            Integer val = columns[columnIndex-1].getInt(curRow-1);
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
        if (curRow <= 0 || curRow > tupleCount)
            throw new SQLException("Current row " + curRow + " does not support operation");
        try {
            Long val = columns[columnIndex-1].getLong(curRow-1);
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
        if (curRow <= 0 || curRow > tupleCount)
            throw new SQLException("Current row " + curRow + " does not support operation");
        try {
            Float val = columns[columnIndex-1].getFloat(curRow-1);
            if (val.isNaN()) {
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
        if (curRow <= 0 || curRow > tupleCount)
            throw new SQLException("Current row " + curRow + " does not support operation");
        try {
            Double val = columns[columnIndex-1].getDouble(curRow-1);
            if (val.isNaN()) {
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
    public byte[] getBytes(int columnIndex) throws SQLException {
        checkNotClosed();
        if (curRow <= 0 || curRow > tupleCount)
            throw new SQLException("Current row " + curRow + " does not support operation");
        try {
            byte[] val = columns[columnIndex-1].getBytes(curRow-1);
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
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        checkNotClosed();
        if (curRow <= 0 || curRow > tupleCount)
            throw new SQLException("Current row " + curRow + " does not support operation");
        try {
            BigDecimal val = columns[columnIndex-1].getBigDecimal(curRow-1);
            if (val == null) {
                lastReadWasNull = true;
                return BigDecimal.ZERO;
            }
            lastReadWasNull = false;
            return val;
        } catch (IndexOutOfBoundsException e) {
            throw new SQLException("columnIndex out of bounds");
        }
    }

    public BigInteger getHugeInt (int columnIndex) throws SQLException {
        checkNotClosed();
        if (curRow <= 0 || curRow > tupleCount)
            throw new SQLException("Current row " + curRow + " does not support operation");
        try {
            BigInteger val = columns[columnIndex-1].getBigInteger(curRow-1);
            if (val == null) {
                lastReadWasNull = true;
                return BigInteger.ZERO;
            }
            lastReadWasNull = false;
            return val;
        } catch (IndexOutOfBoundsException e) {
            throw new SQLException("columnIndex out of bounds");
        }
    }

    public LocalDate getLocalDate(int columnIndex) throws SQLException {
        checkNotClosed();
        if (curRow <= 0 || curRow > tupleCount)
            throw new SQLException("Current row " + curRow + " does not support operation");
        try {
            LocalDate val = columns[columnIndex-1].getLocalDate(curRow-1);
            if (val == null) {
                lastReadWasNull = true;
                return null;
            }
            lastReadWasNull = false;
            return val;
        } catch (IndexOutOfBoundsException e) {
            throw new SQLException("columnIndex out of bounds");
        } catch (DateTimeParseException e) {
            throw new SQLException("DateTime string could not be parsed");
        }
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        LocalDate val = getLocalDate(columnIndex);
        //Set timezone if there is one
        if (cal != null && val != null) {
            val = LocalDateTime.of(val, LocalTime.now())
                    .atZone(cal.getTimeZone().toZoneId())
                    .withZoneSameInstant(ZoneOffset.UTC)
                    .toLocalDate();
        }
        return val != null ? Date.valueOf(val) : null;
    }

    public LocalTime getLocalTime(int columnIndex) throws SQLException {
        checkNotClosed();
        if (curRow <= 0 || curRow > tupleCount)
            throw new SQLException("Current row " + curRow + " does not support operation");
        try {
            LocalTime val = columns[columnIndex-1].getLocalTime(curRow-1);
            if (val == null) {
                lastReadWasNull = true;
                return null;
            }
            lastReadWasNull = false;
            return val;
        } catch (IndexOutOfBoundsException e) {
            throw new SQLException("columnIndex out of bounds");
        } catch (DateTimeParseException e) {
            throw new SQLException("DateTime string could not be parsed");
        }
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        //Get LocalTime without timezone
        LocalTime val = getLocalTime(columnIndex);
        //Set timezone if there is one
        if (cal != null && val != null) {
            val = LocalDateTime.of(LocalDate.now(), val)
                    .atZone(cal.getTimeZone().toZoneId())
                    .withZoneSameInstant(ZoneOffset.UTC)
                    .toLocalTime();
        }
        return val != null ? Time.valueOf(val) : null;
    }

    public LocalDateTime getLocalDateTime(int columnIndex) throws SQLException {
        checkNotClosed();
        if (curRow <= 0 || curRow > tupleCount)
            throw new SQLException("Current row " + curRow + " does not support operation");
        try {
            LocalDateTime val = columns[columnIndex-1].getLocalDateTime(curRow-1);
            if (val == null) {
                lastReadWasNull = true;
                return null;
            }
            lastReadWasNull = false;
            return val;
        } catch (IndexOutOfBoundsException e) {
            throw new SQLException("columnIndex out of bounds");
        } catch (DateTimeParseException e) {
            throw new SQLException("DateTime string could not be parsed");
        }
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        LocalDateTime val = getLocalDateTime(columnIndex);
        //Set timezone if there is one
        if (cal != null && val != null) {
            val = val.atZone(cal.getTimeZone().toZoneId())
                    .withZoneSameInstant(ZoneOffset.UTC)
                    .toLocalDateTime();
        }
        return val != null ? Timestamp.valueOf(val) : null;
    }

    @Override
    public MonetBlob getBlob(int columnIndex) throws SQLException {
        checkNotClosed();
        if (curRow <= 0 || curRow > tupleCount)
            throw new SQLException("Current row " + curRow + " does not support operation");
        try {
            MonetBlob val = columns[columnIndex-1].getBlob(curRow-1);
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
    public Clob getClob(int columnIndex) throws SQLException {
        checkNotClosed();
        if (curRow <= 0 || curRow > tupleCount)
            throw new SQLException("Current row " + curRow + " does not support operation");
        try {
            String val = columns[columnIndex-1].getString(curRow-1);
            if (val == null) {
                lastReadWasNull = true;
                return null;
            }
            lastReadWasNull = false;
            return new MonetClob(val);
        } catch (IndexOutOfBoundsException e) {
            throw new SQLException("columnIndex out of bounds");
        }
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        checkNotClosed();
        if (curRow <= 0 || curRow > tupleCount)
            throw new SQLException("Current row " + curRow + " does not support operation");
        try {
            String val = columns[columnIndex-1].getString(curRow-1);
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

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        checkNotClosed();
        if (curRow <= 0 || curRow > tupleCount)
            throw new SQLException("Current row " + curRow + " does not support operation");
        try {
            Blob val = columns[columnIndex-1].getBlob(curRow-1);
            if (val == null)
                return null;
            return val.getBinaryStream();
        } catch (IndexOutOfBoundsException e) {
            throw new SQLException("columnIndex out of bounds");
        }
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        checkNotClosed();
        if (curRow <= 0 || curRow > tupleCount)
            throw new SQLException("Current row " + curRow + " does not support operation");
        try {
            String val = columns[columnIndex-1].getString(curRow-1);
            if (val == null) {
                lastReadWasNull = true;
                return null;
            }
            lastReadWasNull = false;
            return new java.io.StringReader(val);
        } catch (IndexOutOfBoundsException e) {
            throw new SQLException("columnIndex out of bounds");
        }
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        return getDate(columnIndex,null);
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        return getTime(columnIndex,null);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        return getTimestamp(columnIndex,null);
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
        if (isClosed())
            return;
        this.closed = true;
        MonetNative.monetdbe_result_cleanup(((MonetConnection)this.statement.getConnection()).getDbNative(),nativeResult);
        this.columns = null;
        statement.closeIfComplete();
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        checkNotClosed();
        if (metaData == null) {
            metaData = new MonetResultSetMetaData(columns,columnCount);
        }
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
        else if (row == 0) {
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
        checkNotClosed();
        return statement;
    }

    @Override
    public int getRow() throws SQLException {
        checkNotClosed();
        return curRow;
    }

    @Override
    public int getType() throws SQLException {
        checkNotClosed();
        return resultSetType;
    }

    @Override
    public int getConcurrency() throws SQLException {
        checkNotClosed();
        return concurrency;
    }

    @Override
    public boolean wasNull() throws SQLException {
        checkNotClosed();
        return lastReadWasNull;
    }

    @Override
    public int getHoldability() throws SQLException {
        checkNotClosed();
        return resultSetHoldability;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        checkNotClosed();
        this.fetchDirection = direction;
    }

    @Override
    public int getFetchDirection() throws SQLException {
        checkNotClosed();
        return fetchDirection;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        checkNotClosed();
        this.fetchSize = rows;
    }

    @Override
    public int getFetchSize() throws SQLException {
        checkNotClosed();
        return fetchSize;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        checkNotClosed();
        return warnings;
    }

    @Override
    public void clearWarnings() throws SQLException {
        checkNotClosed();
        this.warnings = new SQLWarning();
    }

    private final void addWarning(final String reason, final String sqlstate) {
        final SQLWarning warn = new SQLWarning(reason, sqlstate);
        if (warnings == null) {
            warnings = warn;
        } else {
            warnings.setNextWarning(warn);
        }
    }

    @Override
    public String getCursorName() throws SQLException {
        checkNotClosed();
        return name;
    }

    //Other gets
    @Override
    @Deprecated
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        checkNotClosed();
        if (scale != 0) {
            addWarning("getBigDecimal(int columnIndex, int scale) is deprecated. Please use getBigDecimal(int columnIndex).setScale(int scale)", "");
            return null;
        }
        else {
            return getBigDecimal(columnIndex);
        }
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        return getString(columnIndex);
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        return getCharacterStream(columnIndex);
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getNClob");
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getSQLXML");
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getAsciiStream");
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getUnicodeStream");
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

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return getDate(findColumn(columnLabel),cal);
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return getTime(findColumn(columnLabel),cal);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return getTimestamp(findColumn(columnLabel),cal);
    }

    public BigInteger getHugeInt (String columnLabel) throws SQLException {
        return getHugeInt(findColumn(columnLabel));
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getObject(findColumn(columnLabel));
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        return getObject(findColumn(columnLabel),map);
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return getObject(findColumn(columnLabel),type);
    }

    //Update
    @Override
    public boolean rowUpdated() throws SQLException {
        throw new SQLFeatureNotSupportedException("update");
    }

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
