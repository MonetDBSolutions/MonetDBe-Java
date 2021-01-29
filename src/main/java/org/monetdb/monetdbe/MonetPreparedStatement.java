package org.monetdb.monetdbe;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.nio.ByteBuffer;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

public class MonetPreparedStatement extends MonetStatement implements PreparedStatement {
    //Native pointer to C statement
    protected ByteBuffer statementNative;
    private MonetParameterMetaData parameterMetaData;

    //Set within monetdbe_prepare
    protected int nParams;
    protected int[] monetdbeTypes;

    //For executeBatch functions
    private Object[] parameters;
    private List<Object[]> parametersBatch = null;

    public MonetPreparedStatement(MonetConnection conn, String sql) {
        super(conn);
        try {
            setPoolable(true);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        //nParams and monetdbeTypes are set within monetdbe_prepare
        this.statementNative = MonetNative.monetdbe_prepare(conn.getDbNative(),sql, this);

        if (nParams > 0) {
            this.parameterMetaData = new MonetParameterMetaData(nParams,monetdbeTypes);
            this.parameters = new Object[nParams];
        }
    }

    //Executes
    @Override
    public boolean execute() throws SQLException {
        checkNotClosed();
        this.resultSet = MonetNative.monetdbe_execute(statementNative,this, false);
        if (this.resultSet!=null) {
            return true;
        }
        else if (this.updateCount != -1){
            return false;
        }
        else {
            //TODO Improve this (happens when an error message is sent by the server, p.e. not all parameters are set)
            throw new SQLException("Server error");
        }
    }

    /** override the execute from the Statement to throw an SQLException */
    @Override
    public boolean execute(final String q) throws SQLException {
        throw new SQLException("This method is not available in a PreparedStatement!", "M1M05");
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        if (!execute())
            throw new SQLException("Query did not produce a result set", "M1M19");
        return getResultSet();
    }

    /** override the executeQuery from the Statement to throw an SQLException */
    @Override
    public ResultSet executeQuery(final String q) throws SQLException {
        throw new SQLException("This method is not available in a PreparedStatement!", "M1M05");
    }

    @Override
    public int executeUpdate() throws SQLException {
        if (execute())
            throw new SQLException("Query produced a result set", "M1M17");
        return getUpdateCount();
    }

    /** override the executeUpdate from the Statement to throw an SQLException */
    @Override
    public int executeUpdate(final String q) throws SQLException {
        throw new SQLException("This method is not available in a PreparedStatement!", "M1M05");
    }

    @Override
    public void addBatch() throws SQLException {
        checkNotClosed();
        //This allows us to add multiple "versions" of the same query, using different parameters
        if (parametersBatch == null) {
            parametersBatch = new ArrayList<>();
        }
        parametersBatch.add(parameters);
        parameters = new Object[nParams];
    }

    /** override the addBatch from the Statement to throw an SQLException */
    @Override
    public void addBatch(final String q) throws SQLException {
        throw new SQLException("This method is not available in a PreparedStatement!", "M1M05");
    }

    //Overrides Statement's implementation, which batches different queries instead of different parameters for same query
    @Override
    public int[] executeBatch() throws SQLException {
        if (parametersBatch == null || parametersBatch.isEmpty()) {
            return new int[0];
        }
        int[] counts = new int[parametersBatch.size()];
        int count = -1;
        Object[] cur_batch;
        Object x;

        for (int i = 0; i < parametersBatch.size(); i++) {
            //Get batch of parameters
            cur_batch = parametersBatch.get(i);

            for (int j = 0; j < nParams; j++) {
                //Set each parameter in current batch
                x = cur_batch[j];
                setObject(j+1,x);
            }

            try {
                count = executeUpdate();
            } catch (SQLException e) {
                //Query returned a resultSet, throw BatchUpdateException
                throw new BatchUpdateException();
            }
            if (count >= 0) {
                counts[i] = count;
            }
            else {
                counts[i] = Statement.SUCCESS_NO_INFO;
            }
        }
        clearBatch();
        return counts;
    }

    //Overrides Statement's implementation, which batches different queries instead of different parameters for same query
    @Override
    public void clearBatch() throws SQLException {
        checkNotClosed();
        parametersBatch = null;
    }

    @Override
    public long executeLargeUpdate() throws SQLException {
        checkNotClosed();
        this.resultSet = MonetNative.monetdbe_execute(statementNative,this, true);
        if (this.resultSet!=null) {
            throw new SQLException("Query produced a result set", "M1M17");
        }
        else {
            return getLargeUpdateCount();
        }
    }

    //Metadata
    //TODO METADATA
    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        //TODO How do I get the result column names and types to construct the ResultSetMetaData object?
        //
        //Because a PreparedStatement object is precompiled, it is possible to know about the ResultSet object that it will return without having to execute it.
        //Consequently, it is possible to invoke the method getMetaData on a PreparedStatement object rather than waiting to execute i
        return null;
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        return parameterMetaData;
    }

    @Override
    public void clearParameters() throws SQLException {
        checkNotClosed();
        //Verify if I should use the cleanup function or if I should set every parameter to NULL
        //This also cleans up the Prepared Statement
        MonetNative.monetdbe_cleanup_statement(conn.getDbNative(),statementNative);
    }

    //Set objects
    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        checkNotClosed();
        if (parameterIndex > nParams) {
            throw new SQLException("parameterIndex is not valid");
        }
        if (x == null) {
            setNull(parameterIndex, targetSqlType);
        }

        if (x instanceof String) {
            setString(parameterIndex,String.valueOf(x));
        }
        else if (x instanceof BigDecimal ||
                x instanceof Byte ||
                x instanceof Short ||
                x instanceof Integer ||
                x instanceof Long ||
                x instanceof Float ||
                x instanceof Double) {
            Number num = (Number) x;
            setObjectNum(parameterIndex,targetSqlType,num,x,scaleOrLength);
        }
        else if (x instanceof Boolean) {
            boolean bool = (Boolean) x;
            setObjectBool(parameterIndex,targetSqlType,bool);
        }
        else if (x instanceof BigInteger) {
            BigInteger num = (BigInteger)x;
            switch (targetSqlType) {
                case Types.BIGINT:
                    setLong(parameterIndex, num.longValue());
                    break;
                case Types.CHAR:
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                    setString(parameterIndex, num.toString());
                    break;
                default:
                    throw new SQLException("Conversion not allowed", "M1M05");
            }
        }
        else if (x instanceof byte[]) {
            switch (targetSqlType) {
                case Types.BINARY:
                case Types.VARBINARY:
                case Types.LONGVARBINARY:
                    setBytes(parameterIndex, (byte[]) x);
                    break;
                default:
                    throw new SQLException("Conversion not allowed", "M1M05");
            }
        }
        else if (x instanceof java.sql.Date ||
                x instanceof Timestamp ||
                x instanceof Time ||
                x instanceof Calendar ||
                x instanceof java.util.Date) {
            setObjectDate(parameterIndex,targetSqlType,x);
        }
        else if (x instanceof MonetBlob || x instanceof Blob) {
            setBlob(parameterIndex, (Blob) x);
        }
        else if (x instanceof java.net.URL) {
            setURL(parameterIndex,(URL) x);
        }
    }

    public void setObjectBool (int parameterIndex, int sqlType, Boolean bool) throws SQLException {
        switch (sqlType) {
            case Types.TINYINT:
                setByte(parameterIndex, (byte)(bool ? 1 : 0));
                break;
            case Types.SMALLINT:
                setShort(parameterIndex, (short)(bool ? 1 : 0));
                break;
            case Types.INTEGER:
                setInt(parameterIndex, (bool ? 1 : 0));  // do not cast to (int) as it generates a compiler warning
                break;
            case Types.BIGINT:
                setLong(parameterIndex, (long)(bool ? 1 : 0));
                break;
            case Types.REAL:
            case Types.FLOAT:
                setFloat(parameterIndex, (float)(bool ? 1.0 : 0.0));
                break;
            case Types.DOUBLE:
                setDouble(parameterIndex, (bool ? 1.0 : 0.0));  // do no cast to (double) as it generates a compiler warning
                break;
            case Types.DECIMAL:
            case Types.NUMERIC:
            {
                final BigDecimal dec;
                try {
                    dec = new BigDecimal(bool ? 1.0 : 0.0);
                } catch (NumberFormatException e) {
                    throw new SQLException("Internal error: unable to create template BigDecimal: " + e.getMessage(), "M0M03");
                }
                setBigDecimal(parameterIndex, dec);
            } break;
            case Types.BIT:
            case Types.BOOLEAN:
                setBoolean(parameterIndex, bool);
                break;
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                setString(parameterIndex, bool.toString());
                break;
            default:
                throw new SQLException("Conversion not allowed", "M1M05");
        }
    }

    public void setObjectNum (int parameterIndex, int sqlType, Number num, Object x, int scale) throws SQLException {
        switch (sqlType) {
            case Types.TINYINT:
                setByte(parameterIndex, num.byteValue());
                break;
            case Types.SMALLINT:
                setShort(parameterIndex, num.shortValue());
                break;
            case Types.INTEGER:
                setInt(parameterIndex, num.intValue());
                break;
            case Types.BIGINT:
                setLong(parameterIndex, num.longValue());
                break;
            case Types.REAL:
            case Types.FLOAT:
                setFloat(parameterIndex, num.floatValue());
                break;
            case Types.DOUBLE:
                setDouble(parameterIndex, num.doubleValue());
                break;
            case Types.DECIMAL:
            case Types.NUMERIC:
                if (x instanceof BigDecimal) {
                    setBigDecimal(parameterIndex, (BigDecimal) x);
                } else {
                    if (scale == 0) {
                        setBigDecimal(parameterIndex, new BigDecimal(num.doubleValue()));
                    }
                    else {
                        setBigDecimal(parameterIndex, new BigDecimal(num.doubleValue()).setScale(scale,java.math.RoundingMode.HALF_UP));
                    }
                }
                break;
            case Types.BIT:
            case Types.BOOLEAN:
                if (num.doubleValue() != 0.0) {
                    setBoolean(parameterIndex, true);
                } else {
                    setBoolean(parameterIndex, false);
                }
                break;
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                setString(parameterIndex, num.toString());
                break;
            default:
                throw new SQLException("Conversion not allowed", "M1M05");
        }
    }

    public void setObjectDate (int parameterIndex, int sqlType, Object x) throws SQLException {
        switch (sqlType) {
            case Types.DATE:
                if (x instanceof java.sql.Date) {
                    setDate(parameterIndex, (java.sql.Date) x);
                } else if (x instanceof Timestamp) {
                    setDate(parameterIndex, new java.sql.Date(((Timestamp)x).getTime()));
                } else if (x instanceof java.util.Date) {
                    setDate(parameterIndex, new java.sql.Date(
                            ((java.util.Date)x).getTime()));
                } else if (x instanceof Calendar) {
                    setDate(parameterIndex, new java.sql.Date(
                            ((Calendar)x).getTimeInMillis()));
                } else {
                    throw new SQLException("Conversion not allowed", "M1M05");
                }
                break;
            case Types.TIME:
                if (x instanceof Time) {
                    setTime(parameterIndex, (Time)x);
                } else if (x instanceof Timestamp) {
                    setTime(parameterIndex, new Time(((Timestamp)x).getTime()));
                } else if (x instanceof java.util.Date) {
                    setTime(parameterIndex, new java.sql.Time(
                            ((java.util.Date)x).getTime()));
                } else if (x instanceof Calendar) {
                    setTime(parameterIndex, new java.sql.Time(
                            ((Calendar)x).getTimeInMillis()));
                } else {
                    throw new SQLException("Conversion not allowed", "M1M05");
                }
                break;
            case Types.TIMESTAMP:
                if (x instanceof Timestamp) {
                    setTimestamp(parameterIndex, (Timestamp)x);
                } else if (x instanceof java.sql.Date) {
                    setTimestamp(parameterIndex, new Timestamp(((java.sql.Date)x).getTime()));
                } else if (x instanceof java.util.Date) {
                    setTimestamp(parameterIndex, new java.sql.Timestamp(
                            ((java.util.Date)x).getTime()));
                } else if (x instanceof Calendar) {
                    setTimestamp(parameterIndex, new java.sql.Timestamp(
                            ((Calendar)x).getTimeInMillis()));
                } else {
                    throw new SQLException("Conversion not allowed", "M1M05");
                }
                break;
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.CLOB:
                setString(parameterIndex, x.toString());
                break;
            default:
                throw new SQLException("Conversion not allowed", "M1M05");
        }
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        checkNotClosed();
        int monettype = MonetTypes.getMonetTypeIntFromSQL(sqlType);
        MonetNative.monetdbe_bind_null(conn.getDbNative(),monettype,statementNative,parameterIndex);
        parameters[parameterIndex-1] = null;
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        checkNotClosed();
        MonetNative.monetdbe_bind(statementNative,x,0,parameterIndex);
        parameters[parameterIndex-1] = x;
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        checkNotClosed();
        MonetNative.monetdbe_bind(statementNative,x,1,parameterIndex);
        parameters[parameterIndex-1] = x;
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        checkNotClosed();
        MonetNative.monetdbe_bind(statementNative,x,2,parameterIndex);
        parameters[parameterIndex-1] = x;
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        checkNotClosed();
        MonetNative.monetdbe_bind(statementNative,x,3,parameterIndex);
        parameters[parameterIndex-1] = x;
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        checkNotClosed();
        MonetNative.monetdbe_bind(statementNative,x,4,parameterIndex);
        parameters[parameterIndex-1] = x;
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        checkNotClosed();
        MonetNative.monetdbe_bind(statementNative,x,7,parameterIndex);
        parameters[parameterIndex-1] = x;
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        checkNotClosed();
        MonetNative.monetdbe_bind(statementNative,x,8,parameterIndex);
        parameters[parameterIndex-1] = x;
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        checkNotClosed();
        Number numberBind;
        //Check unscaled value data type
        BigInteger unscaled = x.unscaledValue();
        int type;
        int bitLenght = unscaled.bitLength();

        if (bitLenght <= 8) {
            numberBind = unscaled.byteValueExact();
            type = 1;
        }
        else if (bitLenght <= 16) {
            numberBind = unscaled.shortValueExact();
            type = 2;
        }
        else if (bitLenght <= 32) {
            numberBind = unscaled.intValueExact();
            type = 3;
        }
        else if (bitLenght <= 64) {
            numberBind = unscaled.longValueExact();
            type = 4;
        }
        else {
            //TODO What to do if it only fits into int128?
            numberBind = unscaled;
            type = 5;
        }
        MonetNative.monetdbe_bind_decimal(statementNative,numberBind,type,x.scale(),parameterIndex);
        parameters[parameterIndex-1] = x;
    }

    //TODO Implement the C function
    public void setHugeInteger(int parameterIndex, BigInteger x) throws SQLException {
        checkNotClosed();
        MonetNative.monetdbe_bind(statementNative,x,5,parameterIndex);
        parameters[parameterIndex-1] = x;
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        checkNotClosed();
        MonetNative.monetdbe_bind(statementNative,x,9,parameterIndex);
        parameters[parameterIndex-1] = x;
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        checkNotClosed();
        LocalDate localDate = x.toLocalDate();
        //MonetNative.monetdbe_bind_date(statementNative,parameterIndex,localDate.getYear(),localDate.getMonthValue(),localDate.getDayOfMonth());
        MonetNative.monetdbe_bind_date(statementNative,parameterIndex,(short)localDate.getYear(),(byte)localDate.getMonthValue(),(byte)localDate.getDayOfMonth());
        parameters[parameterIndex-1] = x;
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        checkNotClosed();
        LocalTime localTime = x.toLocalTime();
        MonetNative.monetdbe_bind_time(statementNative,parameterIndex,localTime.getHour(),localTime.getMinute(),localTime.getSecond(),localTime.getNano()*1000);
        parameters[parameterIndex-1] = x;
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        checkNotClosed();
        LocalDateTime localDateTime = x.toLocalDateTime();
        MonetNative.monetdbe_bind_timestamp(statementNative,parameterIndex,localDateTime.getYear(),localDateTime.getMonthValue(),localDateTime.getDayOfMonth(),localDateTime.getHour(),localDateTime.getMinute(),localDateTime.getSecond(),localDateTime.getNano()*1000);
        parameters[parameterIndex-1] = x;
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        MonetNative.monetdbe_bind(statementNative,x,10,parameterIndex);
        parameters[parameterIndex-1] = x;
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        checkNotClosed();
        byte[] blob_data = x.getBytes(1,(int)x.length());
        MonetNative.monetdbe_bind(statementNative,blob_data,10,parameterIndex);
        parameters[parameterIndex-1] = x;
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        //Ignore typeName parameter, no support for Ref and UDFs in monetdbe
        setNull(parameterIndex,sqlType);
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        //Because MonetDBe doesn't support timezones, the Calendar object is ignored
        setDate(parameterIndex,x);
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        //Because MonetDBe doesn't support timezones, the Calendar object is ignored
        setTime(parameterIndex,x);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        //Because MonetDBe doesn't support timezones, the Calendar object is ignored
        setTimestamp(parameterIndex,x);
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        checkNotClosed();
        setString(parameterIndex,x.toString());
        parameters[parameterIndex-1] = x;
    }

    @Override
    public void setObject(int parameterIndex, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        setObject(parameterIndex,x,MonetTypes.getSQLIntFromSQLName(targetSqlType.getName()),scaleOrLength);
    }

    @Override
    public void setObject(int parameterIndex, Object x, SQLType targetSqlType) throws SQLException {
        setObject(parameterIndex,x,targetSqlType,0);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        setObject(parameterIndex,x,targetSqlType,0);
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        int sqltype = MonetTypes.getDefaultSQLTypeForClass(x.getClass());
        setObject(parameterIndex,x,sqltype,0);
    }

    //Set other objects (Ref, Array, NString, NClob, XML)
    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException("setBlob(int parameterIndex, InputStream inputStream)");
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("setBlob(int parameterIndex, InputStream inputStream, long lenght)");
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("setClob");
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("setClob");
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("setClob");
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        setString(parameterIndex,value);
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException("setRef");
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException("setSQLXML");
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException("setArray");
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException("setRowId");
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        throw new SQLFeatureNotSupportedException("setNClob");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("setNClob");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("setNClob");
    }

    //Set stream object
    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("setCharacterStream");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("setCharacterStream");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("setCharacterStream");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("setAsciiStream");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("setAsciiStream");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("setAsciiStream");
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("setUnicodeStream");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("setBinaryStream");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("setBinaryStream");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("setBinaryStream");
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        setCharacterStream(parameterIndex,value);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        setCharacterStream(parameterIndex,value,length);
    }
}
