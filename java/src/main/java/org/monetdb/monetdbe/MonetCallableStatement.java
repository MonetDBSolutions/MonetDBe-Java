package org.monetdb.monetdbe;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.Map;

public class MonetCallableStatement extends MonetPreparedStatement implements CallableStatement {
    public MonetCallableStatement(MonetConnection conn, String sql) {
        super(conn,removeEscapes(sql));
    }

    /** parse call query string on
     *  { [?=] call <procedure-name> [(<arg1>,<arg2>, ...)] }
     * and remove the JDBC escapes pairs: { and }
     */
    final private static String removeEscapes(final String query) {
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

    /** utility method to convert a parameter name to an int (which represents the parameter index)
     *  this will only succeed for strings like: "1", "2", "3", etc
     *  throws SQLException if it cannot convert the string to an integer number
     */
    final private int nameToIndex(final String parameterName) throws SQLException {
        if (parameterName == null)
            throw new SQLException("Missing parameterName value", "22002");
        try {
            return Integer.parseInt(parameterName);
        } catch (NumberFormatException nfe) {
            throw new SQLException("Cannot convert parameterName '" + parameterName + "' to integer value", "22010");
        }
    }

    //Set object
    @Override
    public void setObject(String parameterName, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        setObject(nameToIndex(parameterName),x,targetSqlType,scaleOrLength);
    }

    @Override
    public void setObject(String parameterName, Object x, SQLType targetSqlType) throws SQLException {
        setObject(nameToIndex(parameterName),x,targetSqlType);
    }

    @Override
    public void setObject(String parameterName, Object x, int targetSqlType, int scale) throws SQLException {
        setObject(nameToIndex(parameterName),x,targetSqlType,scale);
    }

    @Override
    public void setObject(String parameterName, Object x, int targetSqlType) throws SQLException {
        setObject(nameToIndex(parameterName),x,targetSqlType);
    }

    @Override
    public void setObject(String parameterName, Object x) throws SQLException {
        setObject(nameToIndex(parameterName),x);
    }

    @Override
    public void setURL(String parameterName, URL val) throws SQLException {
        setURL(nameToIndex(parameterName),val);
    }

    @Override
    public void setNull(String parameterName, int sqlType) throws SQLException {
        setNull(nameToIndex(parameterName),sqlType);
    }

    @Override
    public void setNull(String parameterName, int sqlType, String typeName) throws SQLException {
        setNull(nameToIndex(parameterName),sqlType,typeName);
    }

    @Override
    public void setBoolean(String parameterName, boolean x) throws SQLException {
        setBoolean(nameToIndex(parameterName),x);
    }

    @Override
    public void setByte(String parameterName, byte x) throws SQLException {
        setByte(nameToIndex(parameterName),x);
    }

    @Override
    public void setShort(String parameterName, short x) throws SQLException {
        setShort(nameToIndex(parameterName),x);
    }

    @Override
    public void setInt(String parameterName, int x) throws SQLException {
        setInt(nameToIndex(parameterName),x);
    }

    @Override
    public void setLong(String parameterName, long x) throws SQLException {
        setLong(nameToIndex(parameterName),x);
    }

    @Override
    public void setFloat(String parameterName, float x) throws SQLException {
        setFloat(nameToIndex(parameterName),x);
    }

    @Override
    public void setDouble(String parameterName, double x) throws SQLException {
        setDouble(nameToIndex(parameterName),x);
    }

    @Override
    public void setBigDecimal(String parameterName, BigDecimal x) throws SQLException {
        setBigDecimal(nameToIndex(parameterName),x);
    }

    @Override
    public void setString(String parameterName, String x) throws SQLException {
        setString(nameToIndex(parameterName),x);
    }

    @Override
    public void setBytes(String parameterName, byte[] x) throws SQLException {
        setBytes(nameToIndex(parameterName),x);
    }

    @Override
    public void setDate(String parameterName, Date x) throws SQLException {
        setDate(nameToIndex(parameterName),x);
    }

    @Override
    public void setTime(String parameterName, Time x) throws SQLException {
        setTime(nameToIndex(parameterName),x);
    }

    @Override
    public void setTimestamp(String parameterName, Timestamp x) throws SQLException {
        setTimestamp(nameToIndex(parameterName),x);
    }

    @Override
    public void setDate(String parameterName, Date x, Calendar cal) throws SQLException {
        setDate(nameToIndex(parameterName),x,cal);
    }

    @Override
    public void setTime(String parameterName, Time x, Calendar cal) throws SQLException {
        setTime(nameToIndex(parameterName),x,cal);
    }

    @Override
    public void setTimestamp(String parameterName, Timestamp x, Calendar cal) throws SQLException {
        setTimestamp(nameToIndex(parameterName),x,cal);
    }

    @Override
    public void setBlob(String parameterName, Blob x) throws SQLException {
        setBlob(nameToIndex(parameterName),x);
    }

    @Override
    public void setClob(String parameterName, Clob x) throws SQLException {
        setClob(nameToIndex(parameterName),x);
    }

    @Override
    public void setClob(String parameterName, Reader reader) throws SQLException {
        setClob(nameToIndex(parameterName),reader);
    }

    @Override
    public void setBlob(String parameterName, InputStream inputStream) throws SQLException {
        setBlob(nameToIndex(parameterName),inputStream);
    }

    @Override
    public void setNClob(String parameterName, Reader reader) throws SQLException {
        setNClob(nameToIndex(parameterName),reader);
    }

    @Override
    public void setRowId(String parameterName, RowId x) throws SQLException {
        setRowId(nameToIndex(parameterName),x);
    }

    @Override
    public void setNString(String parameterName, String value) throws SQLException {
        setNString(nameToIndex(parameterName),value);
    }

    @Override
    public void setNClob(String parameterName, NClob value) throws SQLException {
        setNClob(nameToIndex(parameterName),value);
    }

    @Override
    public void setClob(String parameterName, Reader reader, long length) throws SQLException {
        setNClob(nameToIndex(parameterName),reader,length);
    }

    @Override
    public void setBlob(String parameterName, InputStream inputStream, long length) throws SQLException {
        setBlob(nameToIndex(parameterName),inputStream,length);
    }

    @Override
    public void setNClob(String parameterName, Reader reader, long length) throws SQLException {
        setNClob(nameToIndex(parameterName),reader,length);
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x, int length) throws SQLException {
        setAsciiStream(nameToIndex(parameterName),x,length);
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x, int length) throws SQLException {
        setBinaryStream(nameToIndex(parameterName),x,length);
    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader, int length) throws SQLException {
        setCharacterStream(nameToIndex(parameterName),reader,length);
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x, long length) throws SQLException {
        setAsciiStream(nameToIndex(parameterName),x,length);
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x, long length) throws SQLException {
        setBinaryStream(nameToIndex(parameterName),x,length);
    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader, long length) throws SQLException {
        setCharacterStream(nameToIndex(parameterName),reader,length);
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x) throws SQLException {
        setAsciiStream(nameToIndex(parameterName),x);
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x) throws SQLException {
        setBinaryStream(nameToIndex(parameterName),x);
    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader) throws SQLException {
        setCharacterStream(nameToIndex(parameterName),reader);
    }

    @Override
    public void setNCharacterStream(String parameterName, Reader value) throws SQLException {
        setNCharacterStream(nameToIndex(parameterName),value);
    }

    @Override
    public void setNCharacterStream(String parameterName, Reader value, long length) throws SQLException {
        setNCharacterStream(nameToIndex(parameterName),value,length);
    }

    //Out parameter
    @Override
    public void registerOutParameter(int parameterIndex, SQLType sqlType) throws SQLException {
        throw new SQLFeatureNotSupportedException("registerOutParameter");
    }

    @Override
    public void registerOutParameter(int parameterIndex, SQLType sqlType, int scale) throws SQLException {
        throw new SQLFeatureNotSupportedException("registerOutParameter");
    }

    @Override
    public void registerOutParameter(int parameterIndex, SQLType sqlType, String typeName) throws SQLException {
        throw new SQLFeatureNotSupportedException("registerOutParameter");
    }

    @Override
    public void registerOutParameter(String parameterName, SQLType sqlType) throws SQLException {
        throw new SQLFeatureNotSupportedException("registerOutParameter");
    }

    @Override
    public void registerOutParameter(String parameterName, SQLType sqlType, int scale) throws SQLException {
        throw new SQLFeatureNotSupportedException("registerOutParameter");
    }

    @Override
    public void registerOutParameter(String parameterName, SQLType sqlType, String typeName) throws SQLException {
        throw new SQLFeatureNotSupportedException("registerOutParameter");
    }

    @Override
    public void registerOutParameter(int parameterIndex, int sqlType) throws SQLException {
        throw new SQLFeatureNotSupportedException("registerOutParameter");
    }

    @Override
    public void registerOutParameter(int parameterIndex, int sqlType, int scale) throws SQLException {
        throw new SQLFeatureNotSupportedException("registerOutParameter");
    }

    @Override
    public void registerOutParameter(int parameterIndex, int sqlType, String typeName) throws SQLException {
        throw new SQLFeatureNotSupportedException("registerOutParameter");
    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType) throws SQLException {
        throw new SQLFeatureNotSupportedException("registerOutParameter");
    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType, int scale) throws SQLException {
        throw new SQLFeatureNotSupportedException("registerOutParameter");
    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType, String typeName) throws SQLException {
        throw new SQLFeatureNotSupportedException("registerOutParameter");
    }

    @Override
    public boolean wasNull() throws SQLException {
        throw new SQLFeatureNotSupportedException("wasNull");
    }

    //Get object
    @Override
    public String getString(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getString");
    }

    @Override
    public boolean getBoolean(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getString");
    }

    @Override
    public byte getByte(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getByte");
    }

    @Override
    public short getShort(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getShort");
    }

    @Override
    public int getInt(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getInt");
    }

    @Override
    public long getLong(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getLong");
    }

    @Override
    public float getFloat(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getFloat");
    }

    @Override
    public double getDouble(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getDouble");
    }

    @Override
    public BigDecimal getBigDecimal(int parameterIndex, int scale) throws SQLException {
        throw new SQLFeatureNotSupportedException("getBigDecimal");
    }

    @Override
    public byte[] getBytes(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getBytes");
    }

    @Override
    public Date getDate(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getDate");
    }

    @Override
    public Time getTime(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getTime");
    }

    @Override
    public Timestamp getTimestamp(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getTimestamp");
    }

    @Override
    public Object getObject(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getObject");
    }

    @Override
    public BigDecimal getBigDecimal(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getBigDecimal");
    }

    @Override
    public Object getObject(int parameterIndex, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("getObject");
    }

    @Override
    public Ref getRef(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getRef");
    }

    @Override
    public Blob getBlob(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getBlob");
    }

    @Override
    public Clob getClob(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getClob");
    }

    @Override
    public Array getArray(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getArray");
    }

    @Override
    public Date getDate(int parameterIndex, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("getDate");
    }

    @Override
    public Time getTime(int parameterIndex, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("getTime");
    }

    @Override
    public Timestamp getTimestamp(int parameterIndex, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("getTimestamp");
    }

    @Override
    public URL getURL(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getURL");
    }

    @Override
    public String getString(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("getString");
    }

    @Override
    public boolean getBoolean(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("getString");
    }

    @Override
    public byte getByte(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("getByte");
    }

    @Override
    public short getShort(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("getShort");
    }

    @Override
    public int getInt(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("getInt");
    }

    @Override
    public long getLong(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("getLong");
    }

    @Override
    public float getFloat(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("getFloat");
    }

    @Override
    public double getDouble(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("getDouble");
    }

    @Override
    public byte[] getBytes(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("getBytes");
    }

    @Override
    public Date getDate(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("getDate");
    }

    @Override
    public Time getTime(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("getTime");
    }

    @Override
    public Timestamp getTimestamp(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("getTimestamp");
    }

    @Override
    public Object getObject(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("getObject");
    }

    @Override
    public BigDecimal getBigDecimal(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("getBigDecimal");
    }

    @Override
    public Object getObject(String parameterName, Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("getObject");
    }

    @Override
    public Ref getRef(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("getRef");
    }

    @Override
    public Blob getBlob(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("getBlob");
    }

    @Override
    public Clob getClob(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("getClob");
    }

    @Override
    public Array getArray(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("getArray");
    }

    @Override
    public Date getDate(String parameterName, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("getDate");
    }

    @Override
    public Time getTime(String parameterName, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("getTime");
    }

    @Override
    public Timestamp getTimestamp(String parameterName, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("getTimestamp");
    }

    @Override
    public URL getURL(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("getURL");
    }

    @Override
    public RowId getRowId(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getRowId");
    }

    @Override
    public RowId getRowId(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("getRowId");
    }

    @Override
    public NClob getNClob(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getNClob");
    }

    @Override
    public NClob getNClob(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("getNClob");
    }

    @Override
    public void setSQLXML(String parameterName, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException("getSQLXML");
    }

    @Override
    public SQLXML getSQLXML(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getSQLXML");
    }

    @Override
    public SQLXML getSQLXML(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("getSQLXML");
    }

    @Override
    public String getNString(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getNString");
    }

    @Override
    public String getNString(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("getNString");
    }

    @Override
    public Reader getNCharacterStream(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getNCharacterStream");
    }

    @Override
    public Reader getNCharacterStream(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("getNCharacterStream");
    }

    @Override
    public Reader getCharacterStream(int parameterIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("getCharacterStream");
    }

    @Override
    public Reader getCharacterStream(String parameterName) throws SQLException {
        throw new SQLFeatureNotSupportedException("getCharacterStream");
    }

    @Override
    public <T> T getObject(int parameterIndex, Class<T> type) throws SQLException {
        throw new SQLFeatureNotSupportedException("getObject");
    }

    @Override
    public <T> T getObject(String parameterName, Class<T> type) throws SQLException {
        throw new SQLFeatureNotSupportedException("getObject");
    }
}
