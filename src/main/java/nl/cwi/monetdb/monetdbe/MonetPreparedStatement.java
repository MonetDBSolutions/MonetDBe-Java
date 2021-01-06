package nl.cwi.monetdb.monetdbe;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.ByteBuffer;
import java.sql.*;
import java.util.Calendar;

//TODO Check if the statement is closed before doing actions which depend on it
public class MonetPreparedStatement extends MonetStatement implements PreparedStatement {
    private ByteBuffer statementNative;

    public MonetPreparedStatement(MonetConnection conn, String sql) {
        super(conn);
        this.statementNative = MonetNative.monetdbe_prepare(conn.getDbNative(),sql);
    }

    //Executes
    @Override
    public boolean execute() throws SQLException {
        this.resultSet = MonetNative.monetdbe_execute(statementNative,this);
        if (this.resultSet!=null) {
            return true;
        }
        else {
            return false;
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
        //TODO
        //This allows us to add multiple "versions" of the same query, using different parameters
    }

    @Override
    public long executeLargeUpdate() throws SQLException {
        //TODO LARGE
        return 0;
    }

    //Metadata
    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        //TODO METADATA
        //Because a PreparedStatement object is precompiled, it is possible to know about the ResultSet object that it will return without having to execute it.
        //Consequently, it is possible to invoke the method getMetaData on a PreparedStatement object rather than waiting to execute i
        return null;
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        //TODO METADATA
        return new MonetParameterMetaData();
    }

    @Override
    public void clearParameters() throws SQLException {
        MonetNative.monetdbe_cleanup_statement(conn.getDbNative(),statementNative);
    }

    //Set objects
    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        //TODO Object
    }

    @Override
    public void setObject(int parameterIndex, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        //TODO Object
    }

    @Override
    public void setObject(int parameterIndex, Object x, SQLType targetSqlType) throws SQLException {
        //TODO Object
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        //TODO Object
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        //TODO Object
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        //TODO NULL
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        //TODO NULL
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        MonetNative.monetdbe_bind(statementNative,x,0,parameterIndex);
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        //TODO Byte (is this type 1 -> int8 or char?)
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        MonetNative.monetdbe_bind(statementNative,x,2,parameterIndex);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        MonetNative.monetdbe_bind(statementNative,x,3,parameterIndex);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        MonetNative.monetdbe_bind(statementNative,x,4,parameterIndex);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        MonetNative.monetdbe_bind(statementNative,x,7,parameterIndex);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        MonetNative.monetdbe_bind(statementNative,x,8,parameterIndex);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        //TODO BIG DECIMAL
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        MonetNative.monetdbe_bind(statementNative,x,9,parameterIndex);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        //TODO Bytes
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        MonetNative.monetdbe_bind(statementNative,x,11,parameterIndex);
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        MonetNative.monetdbe_bind(statementNative,x,12,parameterIndex);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        MonetNative.monetdbe_bind(statementNative,x,13,parameterIndex);
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        //TODO Calendar
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        //TODO Calendar
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        //TODO Calendar
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        //TODO URL
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        //TODO BLOB
    }


    //Set other objects (Ref, Clob, Array, NString, NClob, XML
    //TODO Other objects
    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {

    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {

    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {

    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {

    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {

    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {

    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {

    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {

    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {

    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {

    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {

    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {

    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {

    }

    //Set stream object
    //TODO
    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {

    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {

    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {

    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {

    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {

    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {

    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {

    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {

    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {

    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {

    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {

    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {

    }
}
