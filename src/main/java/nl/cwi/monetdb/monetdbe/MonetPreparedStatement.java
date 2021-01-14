package nl.cwi.monetdb.monetdbe;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.ByteBuffer;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.temporal.TemporalField;
import java.time.temporal.TemporalUnit;
import java.util.Calendar;

//TODO Check if the statement is closed before doing actions which depend on it
//TODO Add check to verify Statement is not closed and parameter number is valid before bind/executes
public class MonetPreparedStatement extends MonetStatement implements PreparedStatement {
    private ByteBuffer statementNative;
    protected int nParams;

    public MonetPreparedStatement(MonetConnection conn, String sql) {
        super(conn);
        try {
            setPoolable(true);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        //nParams is set within monetdbe_prepare
        this.statementNative = MonetNative.monetdbe_prepare(conn.getDbNative(),sql, this);
    }

    //Executes
    @Override
    public boolean execute() throws SQLException {
        //TODO Should I test if all parameters are set? Or leave the server to respond with an error?
        this.resultSet = MonetNative.monetdbe_execute(statementNative,this, false);
        if (this.resultSet!=null) {
            return true;
        }
        else if (this.updateCount != -1){
            return false;
        }
        else {
            //TODO Improve this (happens when an error message is sent by the server)
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
        //TODO
        //This allows us to add multiple "versions" of the same query, using different parameters
    }

    @Override
    public long executeLargeUpdate() throws SQLException {
        this.resultSet = MonetNative.monetdbe_execute(statementNative,this, true);
        if (this.resultSet!=null) {
            throw new SQLException("Query produced a result set", "M1M17");
        }
        else {
            return getLargeUpdateCount();
        }
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
        //TODO Verify if I should use the cleanup function or if I should set every parameter to NULL (and use the cleanup function on close method inherited from Statement)
        //This also cleans up the Prepared Statement
        MonetNative.monetdbe_cleanup_statement(conn.getDbNative(),statementNative);
    }

    //Set objects
    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        int monetType = MonetTypes.getMonetTypeInt(targetSqlType);
        switch (monetType) {
            case 0:
                setBoolean(parameterIndex,(boolean) x);
            case 1:
                setShort(parameterIndex,(short) x);
            case 2:
                setShort(parameterIndex,(short) x);
            case 3:
                setInt(parameterIndex,(int) x);
            case 4:
                setLong(parameterIndex,(long) x);
            case 5:
                //TODO HUGEINT
            case 6:
                setInt(parameterIndex,(int) x);
            case 7:
                setFloat(parameterIndex,(float) x);
            case 8:
                setDouble(parameterIndex,(double) x);
            case 9:
                setString(parameterIndex,(String) x);
            case 10:
                //TODO BLOB
            case 11:
                setDate(parameterIndex,(Date) x);
            case 12:
                setTime(parameterIndex,(Time) x);
            case 13:
                setTimestamp(parameterIndex,(Timestamp) x);
            default:
                //TODO Should this be the default?
                setNull(parameterIndex,targetSqlType);
        }
    }

    @Override
    public void setObject(int parameterIndex, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        //TODO SQLTYPE?
    }

    @Override
    public void setObject(int parameterIndex, Object x, SQLType targetSqlType) throws SQLException {
        //TODO SQLTYPE?
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        setObject(parameterIndex,x,targetSqlType,0);
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        int sqltype = MonetTypes.getTypeForClass(x.getClass());
        setObject(parameterIndex,x,sqltype);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        MonetNative.monetdbe_bind(statementNative,null,MonetTypes.getMonetTypeInt(sqlType),parameterIndex);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        MonetNative.monetdbe_bind(statementNative,x,0,parameterIndex);
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        //TODO Test
        MonetNative.monetdbe_bind(statementNative,x,1,parameterIndex);
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
        LocalDate localDate = x.toLocalDate();
        MonetNative.monetdbe_bind_date(statementNative,parameterIndex,localDate.getYear(),localDate.getMonthValue(),localDate.getDayOfMonth());
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        LocalTime localTime = x.toLocalTime();
        MonetNative.monetdbe_bind_time(statementNative,parameterIndex,localTime.getHour(),localTime.getMinute(),localTime.getSecond(),localTime.getNano()*1000);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        LocalDateTime localDateTime = x.toLocalDateTime();
        //TODO ms
        MonetNative.monetdbe_bind_timestamp(statementNative,parameterIndex,localDateTime.getYear(),localDateTime.getMonthValue(),localDateTime.getDayOfMonth(),localDateTime.getHour(),localDateTime.getMinute(),localDateTime.getSecond(),0);
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
        setString(parameterIndex,x.toString());
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        //TODO BLOB
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        //TODO Ref and UDFs
    }

    //Set other objects (Ref, Clob, Array, NString, NClob, XML)
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
