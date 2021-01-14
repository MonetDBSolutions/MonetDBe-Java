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
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

//TODO Check if the statement is closed before doing actions which depend on it
public class MonetPreparedStatement extends MonetStatement implements PreparedStatement {
    private ByteBuffer statementNative;
    protected int nParams;
    private Object[] parameters;
    private List<Object[]> parametersBatch = null;

    public MonetPreparedStatement(MonetConnection conn, String sql) {
        super(conn);
        try {
            setPoolable(true);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        //nParams is set within monetdbe_prepare
        this.statementNative = MonetNative.monetdbe_prepare(conn.getDbNative(),sql, this);
        if (nParams > 0) {
            this.parameters = new Object[nParams];
        }
    }

    //Executes
    @Override
    public boolean execute() throws SQLException {
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
        //This allows us to add multiple "versions" of the same query, using different parameters
        if (parametersBatch == null) {
            parametersBatch = new ArrayList<>();
        }
        parametersBatch.add(parameters);
        parameters = new Object[nParams];
    }

    //Overrides Statement's implementation, which batches different queries instead of different parameters for same query
    //TODO Test
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
    //TODO setObject conversions
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
        parameters[parameterIndex-1] = null;
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        MonetNative.monetdbe_bind(statementNative,x,0,parameterIndex);
        parameters[parameterIndex-1] = x;
    }

    //TODO Test
    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        MonetNative.monetdbe_bind(statementNative,x,1,parameterIndex);
        parameters[parameterIndex-1] = x;
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        MonetNative.monetdbe_bind(statementNative,x,2,parameterIndex);
        parameters[parameterIndex-1] = x;
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        MonetNative.monetdbe_bind(statementNative,x,3,parameterIndex);
        parameters[parameterIndex-1] = x;
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        MonetNative.monetdbe_bind(statementNative,x,4,parameterIndex);
        parameters[parameterIndex-1] = x;
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        MonetNative.monetdbe_bind(statementNative,x,7,parameterIndex);
        parameters[parameterIndex-1] = x;
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        MonetNative.monetdbe_bind(statementNative,x,8,parameterIndex);
        parameters[parameterIndex-1] = x;
    }

    //TODO BIG DECIMAL
    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {

    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        MonetNative.monetdbe_bind(statementNative,x,9,parameterIndex);
        parameters[parameterIndex-1] = x;
    }

    //TODO Bytes - is there a type which translates to bytes?
    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {

    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        LocalDate localDate = x.toLocalDate();
        MonetNative.monetdbe_bind_date(statementNative,parameterIndex,localDate.getYear(),localDate.getMonthValue(),localDate.getDayOfMonth());
        parameters[parameterIndex-1] = x;
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        LocalTime localTime = x.toLocalTime();
        MonetNative.monetdbe_bind_time(statementNative,parameterIndex,localTime.getHour(),localTime.getMinute(),localTime.getSecond(),localTime.getNano()*1000);
        parameters[parameterIndex-1] = x;
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        LocalDateTime localDateTime = x.toLocalDateTime();
        //TODO ms
        MonetNative.monetdbe_bind_timestamp(statementNative,parameterIndex,localDateTime.getYear(),localDateTime.getMonthValue(),localDateTime.getDayOfMonth(),localDateTime.getHour(),localDateTime.getMinute(),localDateTime.getSecond(),0);
        parameters[parameterIndex-1] = x;
    }

    //TODO Calendar
    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {

    }

    //TODO Calendar
    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {

    }

    //TODO Calendar
    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {

    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        setString(parameterIndex,x.toString());
        parameters[parameterIndex-1] = x;
    }

    //TODO BLOB
    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {

    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {

    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {

    }

    //TODO CLOB
    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {

    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {

    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {

    }
    
    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        //TODO Ref and UDFs?
    }

    //Set other objects (Ref, Array, NString, NClob, XML)
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
