package org.monetdb.monetdbe;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.Map;

/**
 * A {@link CallableStatement} suitable for the MonetDB database.
 *
 * The interface used to execute SQL stored procedures. IN parameter values are set using the set methods inherited from PreparedStatement.
 * MonetDB does not support OUT or INOUT parameters. Only input parameters are supported.
 *
 * This implementation of the CallableStatement interface reuses the implementation of MonetPreparedStatement for
 * preparing the call statement, bind parameter values and execute the call, possibly multiple times with different parameter values.
 *
 * Note: currently we can not implement:
 * - all getXyz(parameterIndex/parameterName, ...) methods
 * - all registerOutParameter(parameterIndex/parameterName, int sqlType, ...) methods
 * - wasNull() method
 * because output parameters in stored procedures are not supported by MonetDB.
 */
public class MonetCallableStatement extends MonetPreparedStatement implements CallableStatement {
    /**
     * MonetCallableStatement constructor which checks the arguments for validity.
     * A MonetCallableStatement is backed by a {@link MonetPreparedStatement}, which deals with most of the required stuff of this class.
     *
     * @param conn the connection that created this Statement
     * @param sql - an SQL CALL statement that may contain one or more '?' parameter placeholders.
     *	Typically this statement is specified using JDBC call escape syntax:
     *	{ call procedure_name [(?,?, ...)] }
     *	or
     *	{ ?= call procedure_name [(?,?, ...)] }
     *
     * @throws IllegalArgumentException is one of the arguments is null or empty
     */
    MonetCallableStatement(MonetConnection conn, String sql) {
        super(conn,removeEscapes(sql));
    }

    /** Parses call query string on
     *  { [?=] call &lt;procedure-name&gt; [(&lt;arg1&gt;,&lt;arg2&gt;, ...)] }
     * and remove the JDBC escapes pairs: { and }
     *
     * @param query Query string to be escaped
     * @return Escaped query string
     */
    private static String removeEscapes(final String query) {
        if (query == null)
            return null;

        final int firstAccOpen = query.indexOf("{");
        if (firstAccOpen == -1)
            // nothing to remove
            return query;

        final int len = query.length();
        final StringBuilder buf = new StringBuilder(len);
        int countAccolades = 0;
        // simple scanner which copies all characters except the first '{' and matching '}' character
        // we currently do not check if 'call' appears after the first '{' and before the '}' character
        // we currently also do not deal correctly with { or } appearing as comment or as part of a string value
        for (int i = 0; i < len; i++) {
            char c = query.charAt(i);
            switch (c) {
                case '{':
                    countAccolades++;
                    if (i == firstAccOpen)
                        continue;
                    else
                        buf.append(c);
                    break;
                case '}':
                    countAccolades--;
                    if (i > firstAccOpen && countAccolades == 0)
                        continue;
                    else
                        buf.append(c);
                    break;
                default:
                    buf.append(c);
            }
        }
        return buf.toString();
    }

    /** Utility method to convert a parameter name to an int (which represents the parameter index).
     *  This will only succeed for strings like: "1", "2", "3", etc
     *
     *  @param parameterName Parameter index as a String.
     *  @return Parameter index as integer.
     *  @throws SQLException if it cannot convert the string to an integer number
     */
    private int nameToIndex(final String parameterName) throws SQLException {
        if (parameterName == null)
            throw new SQLException("Missing parameterName value", "22002");
        try {
            return Integer.parseInt(parameterName);
        } catch (NumberFormatException nfe) {
            throw new SQLException("Cannot convert parameterName '" + parameterName + "' to integer value", "22010");
        }
    }

    //Set object

    /**
     * @see MonetPreparedStatement#setObject(int, Object, SQLType, int)
     */
    @Override
    public void setObject(String parameterName, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        setObject(nameToIndex(parameterName),x,targetSqlType,scaleOrLength);
    }

    /**
     * The parameterName String argument is converted to int by {@link #nameToIndex(String)}.
     * @see MonetPreparedStatement#setObject(int, Object, SQLType)
     */
    @Override
    public void setObject(String parameterName, Object x, SQLType targetSqlType) throws SQLException {
        setObject(nameToIndex(parameterName),x,targetSqlType);
    }

    /**
     * The parameterName String argument is converted to int by {@link #nameToIndex(String)}.
     * @see MonetPreparedStatement#setObject(int, Object, int, int)
     */
    @Override
    public void setObject(String parameterName, Object x, int targetSqlType, int scale) throws SQLException {
        setObject(nameToIndex(parameterName),x,targetSqlType,scale);
    }

    /**
     * The parameterName String argument is converted to int by {@link #nameToIndex(String)}.
     * @see MonetPreparedStatement#setObject(int, Object, int)
     */
    @Override
    public void setObject(String parameterName, Object x, int targetSqlType) throws SQLException {
        setObject(nameToIndex(parameterName),x,targetSqlType);
    }

    /**
     * The parameterName String argument is converted to int by {@link #nameToIndex(String)}.
     * @see MonetPreparedStatement#setObject(int, Object)
     */
    @Override
    public void setObject(String parameterName, Object x) throws SQLException {
        setObject(nameToIndex(parameterName),x);
    }

    /**
     * The parameterName String argument is converted to int by {@link #nameToIndex(String)}.
     * @see MonetPreparedStatement#setURL(int, URL)
     */
    @Override
    public void setURL(String parameterName, URL val) throws SQLException {
        setURL(nameToIndex(parameterName),val);
    }

    /**
     * The parameterName String argument is converted to int by {@link #nameToIndex(String)}.
     * @see MonetPreparedStatement#setNull(int, int)
     */
    @Override
    public void setNull(String parameterName, int sqlType) throws SQLException {
        setNull(nameToIndex(parameterName),sqlType);
    }

    /**
     * The parameterName String argument is converted to int by {@link #nameToIndex(String)}.
     * @see MonetPreparedStatement#setNull(int, int,String)
     */
    @Override
    public void setNull(String parameterName, int sqlType, String typeName) throws SQLException {
        setNull(nameToIndex(parameterName),sqlType,typeName);
    }

    /**
     * The parameterName String argument is converted to int by {@link #nameToIndex(String)}.
     * @see MonetPreparedStatement#setBoolean(int, boolean) 
     */
    @Override
    public void setBoolean(String parameterName, boolean x) throws SQLException {
        setBoolean(nameToIndex(parameterName),x);
    }

    /**
     * The parameterName String argument is converted to int by {@link #nameToIndex(String)}.
     * @see MonetPreparedStatement#setByte(int, byte) 
     */
    @Override
    public void setByte(String parameterName, byte x) throws SQLException {
        setByte(nameToIndex(parameterName),x);
    }

    /**
     * The parameterName String argument is converted to int by {@link #nameToIndex(String)}.
     * @see MonetPreparedStatement#setShort(int, short) 
     */
    @Override
    public void setShort(String parameterName, short x) throws SQLException {
        setShort(nameToIndex(parameterName),x);
    }

    /**
     * The parameterName String argument is converted to int by {@link #nameToIndex(String)}.
     * @see MonetPreparedStatement#setInt(int, int) 
     */
    @Override
    public void setInt(String parameterName, int x) throws SQLException {
        setInt(nameToIndex(parameterName),x);
    }

    /**
     * The parameterName String argument is converted to int by {@link #nameToIndex(String)}.
     * @see MonetPreparedStatement#setLong(int, long) 
     */
    @Override
    public void setLong(String parameterName, long x) throws SQLException {
        setLong(nameToIndex(parameterName),x);
    }

    /**
     * The parameterName String argument is converted to int by {@link #nameToIndex(String)}.
     * @see MonetPreparedStatement#setFloat(int, float) 
     */
    @Override
    public void setFloat(String parameterName, float x) throws SQLException {
        setFloat(nameToIndex(parameterName),x);
    }

    /**
     * The parameterName String argument is converted to int by {@link #nameToIndex(String)}.
     * @see MonetPreparedStatement#setDouble(int, double) 
     */
    @Override
    public void setDouble(String parameterName, double x) throws SQLException {
        setDouble(nameToIndex(parameterName),x);
    }

    /**
     * The parameterName String argument is converted to int by {@link #nameToIndex(String)}.
     * @see MonetPreparedStatement#setBigDecimal(int, BigDecimal) 
     */
    @Override
    public void setBigDecimal(String parameterName, BigDecimal x) throws SQLException {
        setBigDecimal(nameToIndex(parameterName),x);
    }

    /**
     * The parameterName String argument is converted to int by {@link #nameToIndex(String)}.
     * @see MonetPreparedStatement#setString(int, String) 
     */
    @Override
    public void setString(String parameterName, String x) throws SQLException {
        setString(nameToIndex(parameterName),x);
    }

    /**
     * The parameterName String argument is converted to int by {@link #nameToIndex(String)}.
     * @see MonetPreparedStatement#setBytes(int, byte[]) 
     */
    @Override
    public void setBytes(String parameterName, byte[] x) throws SQLException {
        setBytes(nameToIndex(parameterName),x);
    }

    /**
     * The parameterName String argument is converted to int by {@link #nameToIndex(String)}.
     * @see MonetPreparedStatement#setDate(int, Date) 
     */
    @Override
    public void setDate(String parameterName, Date x) throws SQLException {
        setDate(nameToIndex(parameterName),x);
    }

    /**
     * The parameterName String argument is converted to int by {@link #nameToIndex(String)}.
     * @see MonetPreparedStatement#setTime(int, Time) 
     */
    @Override
    public void setTime(String parameterName, Time x) throws SQLException {
        setTime(nameToIndex(parameterName),x);
    }

    /**
     * The parameterName String argument is converted to int by {@link #nameToIndex(String)}.
     * @see MonetPreparedStatement#setTimestamp(int, Timestamp) 
     */
    @Override
    public void setTimestamp(String parameterName, Timestamp x) throws SQLException {
        setTimestamp(nameToIndex(parameterName),x);
    }

    /**
     * The parameterName String argument is converted to int by {@link #nameToIndex(String)}.
     * @see MonetPreparedStatement#setDate(int, Date, Calendar)
     */
    @Override
    public void setDate(String parameterName, Date x, Calendar cal) throws SQLException {
        setDate(nameToIndex(parameterName),x,cal);
    }

    /**
     * The parameterName String argument is converted to int by {@link #nameToIndex(String)}.
     * @see MonetPreparedStatement#setTime(int, Time, Calendar)
     */
    @Override
    public void setTime(String parameterName, Time x, Calendar cal) throws SQLException {
        setTime(nameToIndex(parameterName),x,cal);
    }

    /**
     * The parameterName String argument is converted to int by {@link #nameToIndex(String)}.
     * @see MonetPreparedStatement#setTimestamp(int, Timestamp, Calendar)
     */
    @Override
    public void setTimestamp(String parameterName, Timestamp x, Calendar cal) throws SQLException {
        setTimestamp(nameToIndex(parameterName),x,cal);
    }

    /**
     * The parameterName String argument is converted to int by {@link #nameToIndex(String)}.
     * @see MonetPreparedStatement#setBlob(int, Blob) 
     */
    @Override
    public void setBlob(String parameterName, Blob x) throws SQLException {
        setBlob(nameToIndex(parameterName),x);
    }

    /**
     * The parameterName String argument is converted to int by {@link #nameToIndex(String)}.
     * @see MonetPreparedStatement#setClob(int, Clob) 
     */
    @Override
    public void setClob(String parameterName, Clob x) throws SQLException {
        setClob(nameToIndex(parameterName),x);
    }

    /**
     * The parameterName String argument is converted to int by {@link #nameToIndex(String)}.
     * @see MonetPreparedStatement#setClob(int, Reader) 
     */
    @Override
    public void setClob(String parameterName, Reader reader) throws SQLException {
        setClob(nameToIndex(parameterName),reader);
    }

    /**
     * The parameterName String argument is converted to int by {@link #nameToIndex(String)}.
     * @see MonetPreparedStatement#setBlob(int, InputStream) 
     */
    @Override
    public void setBlob(String parameterName, InputStream inputStream) throws SQLException {
        setBlob(nameToIndex(parameterName),inputStream);
    }

    /**
     * The parameterName String argument is converted to int by {@link #nameToIndex(String)}.
     * @see MonetPreparedStatement#setNClob(int, Reader)
     */
    @Override
    public void setNClob(String parameterName, Reader reader) throws SQLException {
        setNClob(nameToIndex(parameterName),reader);
    }

    /**
     * The parameterName String argument is converted to int by {@link #nameToIndex(String)}.
     * @see MonetPreparedStatement#setRowId(int, RowId) 
     */
    @Override
    public void setRowId(String parameterName, RowId x) throws SQLException {
        setRowId(nameToIndex(parameterName),x);
    }

    /**
     * The parameterName String argument is converted to int by {@link #nameToIndex(String)}.
     * @see MonetPreparedStatement#setNString(int, String) 
     */
    @Override
    public void setNString(String parameterName, String value) throws SQLException {
        setNString(nameToIndex(parameterName),value);
    }

    /**
     * The parameterName String argument is converted to int by {@link #nameToIndex(String)}.
     * @see MonetPreparedStatement#setNClob(int, NClob) 
     */
    @Override
    public void setNClob(String parameterName, NClob value) throws SQLException {
        setNClob(nameToIndex(parameterName),value);
    }

    /**
     * The parameterName String argument is converted to int by {@link #nameToIndex(String)}.
     * @see MonetPreparedStatement#setClob(int, Reader,long)
     */
    @Override
    public void setClob(String parameterName, Reader reader, long length) throws SQLException {
        setNClob(nameToIndex(parameterName),reader,length);
    }

    /**
     * The parameterName String argument is converted to int by {@link #nameToIndex(String)}.
     * @see MonetPreparedStatement#setBlob(int, InputStream,long)
     */
    @Override
    public void setBlob(String parameterName, InputStream inputStream, long length) throws SQLException {
        setBlob(nameToIndex(parameterName),inputStream,length);
    }

    /**
     * The parameterName String argument is converted to int by {@link #nameToIndex(String)}.
     * @see MonetPreparedStatement#setNClob(int, Reader, long) 
     */
    @Override
    public void setNClob(String parameterName, Reader reader, long length) throws SQLException {
        setNClob(nameToIndex(parameterName),reader,length);
    }

    /**
     * The parameterName String argument is converted to int by {@link #nameToIndex(String)}.
     * @see MonetPreparedStatement#setAsciiStream(int, InputStream,long)
     */
    @Override
    public void setAsciiStream(String parameterName, InputStream x, int length) throws SQLException {
        setAsciiStream(nameToIndex(parameterName),x,length);
    }

    /**
     * The parameterName String argument is converted to int by {@link #nameToIndex(String)}.
     * @see MonetPreparedStatement#setBinaryStream(int, InputStream,int)
     */
    @Override
    public void setBinaryStream(String parameterName, InputStream x, int length) throws SQLException {
        setBinaryStream(nameToIndex(parameterName),x,length);
    }

    /**
     * The parameterName String argument is converted to int by {@link #nameToIndex(String)}.
     * @see MonetPreparedStatement#setCharacterStream(int, Reader,int)
     */
    @Override
    public void setCharacterStream(String parameterName, Reader reader, int length) throws SQLException {
        setCharacterStream(nameToIndex(parameterName),reader,length);
    }

    /**
     * The parameterName String argument is converted to int by {@link #nameToIndex(String)}.
     * @see MonetPreparedStatement#setAsciiStream(int, InputStream,long)
     */
    @Override
    public void setAsciiStream(String parameterName, InputStream x, long length) throws SQLException {
        setAsciiStream(nameToIndex(parameterName),x,length);
    }

    /**
     * The parameterName String argument is converted to int by {@link #nameToIndex(String)}.
     * @see MonetPreparedStatement#setBinaryStream(int, InputStream,long)
     */
    @Override
    public void setBinaryStream(String parameterName, InputStream x, long length) throws SQLException {
        setBinaryStream(nameToIndex(parameterName),x,length);
    }

    /**
     * The parameterName String argument is converted to int by {@link #nameToIndex(String)}.
     * @see MonetPreparedStatement#setNCharacterStream(int, Reader, long)
     */
    @Override
    public void setCharacterStream(String parameterName, Reader reader, long length) throws SQLException {
        setCharacterStream(nameToIndex(parameterName),reader,length);
    }

    /**
     * The parameterName String argument is converted to int by {@link #nameToIndex(String)}.
     * @see MonetPreparedStatement#setAsciiStream(int, InputStream) 
     */
    @Override
    public void setAsciiStream(String parameterName, InputStream x) throws SQLException {
        setAsciiStream(nameToIndex(parameterName),x);
    }

    /**
     * The parameterName String argument is converted to int by {@link #nameToIndex(String)}.
     * @see MonetPreparedStatement#setBinaryStream(int, InputStream)
     */
    @Override
    public void setBinaryStream(String parameterName, InputStream x) throws SQLException {
        setBinaryStream(nameToIndex(parameterName),x);
    }

    /**
     * The parameterName String argument is converted to int by {@link #nameToIndex(String)}.
     * @see MonetPreparedStatement#setCharacterStream(int, Reader)
     */
    @Override
    public void setCharacterStream(String parameterName, Reader reader) throws SQLException {
        setCharacterStream(nameToIndex(parameterName),reader);
    }

    /**
     * The parameterName String argument is converted to int by {@link #nameToIndex(String)}.
     * @see MonetPreparedStatement#setNCharacterStream(int, Reader)
     */
    @Override
    public void setNCharacterStream(String parameterName, Reader value) throws SQLException {
        setNCharacterStream(nameToIndex(parameterName),value);
    }

    /**
     * The parameterName String argument is converted to int by {@link #nameToIndex(String)}.
     * @see MonetPreparedStatement#setNCharacterStream(int, Reader, long) 
     */
    @Override
    public void setNCharacterStream(String parameterName, Reader value, long length) throws SQLException {
        setNCharacterStream(nameToIndex(parameterName),value,length);
    }

    //Out parameter
    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public void registerOutParameter(int parameterIndex, SQLType sqlType) throws SQLException {
        throw new SQLFeatureNotSupportedException("registerOutParameter");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public void registerOutParameter(int parameterIndex, SQLType sqlType, int scale) throws SQLException {
        throw new SQLFeatureNotSupportedException("registerOutParameter");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public void registerOutParameter(int parameterIndex, SQLType sqlType, String typeName) throws SQLException {
        throw new SQLFeatureNotSupportedException("registerOutParameter");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public void registerOutParameter(String parameterName, SQLType sqlType) throws SQLException {
        throw new SQLFeatureNotSupportedException("registerOutParameter");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public void registerOutParameter(String parameterName, SQLType sqlType, int scale) throws SQLException {
        throw new SQLFeatureNotSupportedException("registerOutParameter");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public void registerOutParameter(String parameterName, SQLType sqlType, String typeName) throws SQLException {
        throw new SQLFeatureNotSupportedException("registerOutParameter");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public void registerOutParameter(int parameterIndex, int sqlType) throws SQLException {
        throw new SQLFeatureNotSupportedException("registerOutParameter");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public void registerOutParameter(int parameterIndex, int sqlType, int scale) throws SQLException {
        throw new SQLFeatureNotSupportedException("registerOutParameter");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public void registerOutParameter(int parameterIndex, int sqlType, String typeName) throws SQLException {
        throw new SQLFeatureNotSupportedException("registerOutParameter");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public void registerOutParameter(String parameterName, int sqlType) throws SQLException {
        throw new SQLFeatureNotSupportedException("registerOutParameter");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public void registerOutParameter(String parameterName, int sqlType, int scale) throws SQLException {
        throw new SQLFeatureNotSupportedException("registerOutParameter");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public void registerOutParameter(String parameterName, int sqlType, String typeName) throws SQLException {
        throw new SQLFeatureNotSupportedException("registerOutParameter");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public boolean wasNull() throws SQLException {
        throw new SQLFeatureNotSupportedException("wasNull");
    }

    //Get object
    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public String getString(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getString");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public boolean getBoolean(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getString");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public byte getByte(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getByte");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public short getShort(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getShort");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public int getInt(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getInt");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public long getLong(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getLong");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public float getFloat(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getFloat");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public double getDouble(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getDouble");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public BigDecimal getBigDecimal(int parameterIndex, int scale) throws SQLException {
        throw new SQLFeatureNotSupportedException("getBigDecimal");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public byte[] getBytes(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getBytes");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public Date getDate(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getDate");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public Time getTime(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getTime");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public Timestamp getTimestamp(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getTimestamp");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public Object getObject(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getObject");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public BigDecimal getBigDecimal(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getBigDecimal");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public Object getObject(int parameterIndex, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("getObject");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public Ref getRef(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getRef");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public Blob getBlob(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getBlob");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public Clob getClob(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getClob");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public Array getArray(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getArray");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public Date getDate(int parameterIndex, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("getDate");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public Time getTime(int parameterIndex, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("getTime");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public Timestamp getTimestamp(int parameterIndex, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("getTimestamp");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public URL getURL(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getURL");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public String getString(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("getString");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public boolean getBoolean(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("getString");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public byte getByte(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("getByte");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public short getShort(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("getShort");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public int getInt(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("getInt");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public long getLong(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("getLong");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public float getFloat(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("getFloat");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public double getDouble(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("getDouble");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public byte[] getBytes(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("getBytes");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public Date getDate(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("getDate");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public Time getTime(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("getTime");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public Timestamp getTimestamp(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("getTimestamp");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public Object getObject(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("getObject");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public BigDecimal getBigDecimal(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("getBigDecimal");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public Object getObject(String parameterName, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("getObject");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public Ref getRef(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("getRef");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public Blob getBlob(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("getBlob");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public Clob getClob(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("getClob");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public Array getArray(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("getArray");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public Date getDate(String parameterName, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("getDate");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public Time getTime(String parameterName, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("getTime");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public Timestamp getTimestamp(String parameterName, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("getTimestamp");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public URL getURL(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("getURL");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public RowId getRowId(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getRowId");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public RowId getRowId(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("getRowId");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public NClob getNClob(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getNClob");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public NClob getNClob(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("getNClob");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public void setSQLXML(String parameterName, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException("getSQLXML");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public SQLXML getSQLXML(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getSQLXML");
    }

   /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public SQLXML getSQLXML(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("getSQLXML");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public String getNString(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getNString");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public String getNString(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("getNString");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public Reader getNCharacterStream(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getNCharacterStream");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public Reader getNCharacterStream(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("getNCharacterStream");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public Reader getCharacterStream(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getCharacterStream");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public Reader getCharacterStream(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("getCharacterStream");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public <T> T getObject(int parameterIndex, Class<T> type) throws SQLException {
        throw new SQLFeatureNotSupportedException("getObject");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public <T> T getObject(String parameterName, Class<T> type) throws SQLException {
        throw new SQLFeatureNotSupportedException("getObject");
    }
}
