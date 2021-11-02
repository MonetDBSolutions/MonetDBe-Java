package org.monetdb.monetdbe;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

/**
 * A {@link PreparedStatement} suitable for the MonetDB embedded database.
 * <p>
 * An object that represents a precompiled SQL statement. A SQL statement is precompiled and stored in a PreparedStatement object.
 * This object can then be used to efficiently execute this statement multiple times.
 * <p>
 * Note: The setter methods (setShort, setString, and so on) for setting IN parameter values must specify types that are compatible with
 * the defined SQL type of the input parameter. For instance, if the IN parameter has SQL type INTEGER, then the method setInt should be used.
 * If arbitrary parameter type conversions are required, the method setObject should be used with a target SQL type.
 */
public class MonetPreparedStatement extends MonetStatement implements PreparedStatement {
    /* PreparedStatement state variables */
    /** The pointer to the C statement object */
    protected ByteBuffer statementNative;
    /** Currently bound parameters */
    private Object[] parameters;
    /** Array of bound parameters, for use in executeBatch() */
    private List<Object[]> parametersBatch = null;

    /* Input parameters variables */
    /** Metadata object containing info about the input parameters of the prepared statement */
    private MonetParameterMetaData parameterMetaData;
    /** Number of bind-able parameters */
    protected int nParams;
    /** Array of types of bind-able parameters */
    protected int[] monetdbeTypes;
    /** Array of MonetDB GDK internal types of bind-able parameters */
    protected String[] paramMonetGDKTypes;
    /** Digits for bind-able parameters */
    protected int[] digitsInput;
    /** Scales for bind-able parameters */
    protected int[] scaleInput;

    /* Output result variables */
    /** Metadata object containing info about the output columns of the prepared statement */
    private MonetResultSetMetaData resultSetMetaData;
    /** Number of columns in the pre-compiled result set of the PreparedStatement */
    protected int nCols;
    /** MonetDB GDK types for the columns in the pre-compiled result set */
    protected String[] resultMonetGDKTypes;
    /** Column names for the pre-compiled result set */
    protected String[] resultNames;

    /**
     * Prepared statement constructor, calls monetdbe_prepare() and super-class Statement constructor.
     * The prepared statement is destroyed if the monetdbe_prepare() call returned an error.
     *
     * The statementNative variable is set within monetdbe_prepare()
     * If there are input parameters: nParams and paramMonetGDKTypes are set within monetdbe_prepare()
     * If there are output parameters: nCols, resultMonetGDKTypes and resultNames are set within monetdbe_prepare()
     *
     * @param conn parent connection
     * @param sql  query to prepare
     */
    public MonetPreparedStatement(MonetConnection conn, String sql) {
        super(conn);
        String error_msg = MonetNative.monetdbe_prepare(conn.getDbNative(), sql, this);

        //Failed prepare, destroy statement
        if (error_msg != null || this.statementNative == null) {
            if (error_msg != null)
                System.err.println("Prepare statement error: " + error_msg);
            else
                System.err.println("Prepare statement error: statement native object is null");

            try {
                this.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        if (nParams > 0) {
            this.monetdbeTypes = new int[nParams];
            for (int i = 0; i < nParams; i++)
                monetdbeTypes[i] = MonetTypes.getMonetTypeFromGDKType(paramMonetGDKTypes[i]);
            this.parameterMetaData = new MonetParameterMetaData(nParams, monetdbeTypes,digitsInput,scaleInput);
            this.parameters = new Object[nParams];
        }
        else {
            //If there are no parameters, set the variable to null for later checks
            this.parameters = null;
            this.monetdbeTypes = null;
            this.parameterMetaData = null;
        }

        if (nCols > 0) {
            this.resultSetMetaData = new MonetResultSetMetaData(nCols,resultMonetGDKTypes,resultNames);
        }
        else {
            this.resultSetMetaData = null;
        }
    }

    /**
     * Executes the SQL statement in this PreparedStatement object, which may be any kind of SQL statement.
     * The execute method returns a boolean to indicate the form of the first result.
     * You must then use the methods getResultSet or getUpdateCount to retrieve the result
     * (either a ResultSet object or an int representing the update count).
     * <p>
     * Multiple results may result from the SQL statement, but only the first one may be retrieved in the current version.
     *
     * @return true if the first result is a ResultSet object; false if it is an
     * update count or there are no results
     * @throws SQLException if a database access error occurs or an argument is supplied to this method
     */
    @Override
    public boolean execute() throws SQLException {
        checkNotClosed();

        int lastUpdateCount = this.updateCount;
        MonetResultSet lastResultSet = this.resultSet;
        this.resultSet = null;
        this.updateCount = -1;

        //ResultSet and UpdateCount is set within monetdbe_execute
        String error_msg = MonetNative.monetdbe_execute(statementNative, this, false, getMaxRows());
        if (error_msg != null) {
            this.updateCount = lastUpdateCount;
            this.resultSet = lastResultSet;
            throw new SQLException(error_msg);
        } else if (this.resultSet != null) {
            return true;
        } else if (this.updateCount >= 0) {
            return false;
        } else {
            throw new SQLException("No update count or result set returned");
        }
    }

    /**
     * Executes the SQL statement in this PreparedStatement object, which may be any kind of SQL statement.
     * The execute method returns a boolean to indicate the form of the first result.
     * You must then use the methods getResultSet or getUpdateCount to retrieve the result
     * (either a ResultSet object or an int representing the update count).
     * <p>
     * Multiple results may result from the SQL statement, but only the first one may be retrieved in the current version.
     * <p>
     * This method should be used when the returned row count may exceed Integer.MAX_VALUE.
     *
     * @return true if the first result is a ResultSet object; false if it is an
     * update count or there are no results
     * @throws SQLException if a database access error occurs or an argument is supplied to this method
     */
    @Override
    public long executeLargeUpdate() throws SQLException {
        checkNotClosed();

        long lastUpdateCount = this.largeUpdateCount;
        MonetResultSet lastResultSet = this.resultSet;
        this.resultSet = null;
        this.largeUpdateCount = -1;

        //ResultSet and UpdateCount is set within monetdbe_execute
        String error_msg = MonetNative.monetdbe_execute(statementNative, this, true, getMaxRows());
        if (error_msg != null) {
            this.largeUpdateCount = lastUpdateCount;
            this.resultSet = lastResultSet;
            throw new SQLException(error_msg);
        } else if (this.resultSet != null) {
            throw new SQLException("Query produced a result set", "M1M17");
        } else {
            return getLargeUpdateCount();
        }
    }

    /**
     * Override the execute from the Statement to throw a SQLException
     */
    @Override
    public boolean execute(final String q) throws SQLException {
        throw new SQLException("This method is not available in a PreparedStatement!", "M1M05");
    }

    /**
     * Executes the SQL query in this PreparedStatement object and returns the ResultSet object generated by the query.
     *
     * @return a ResultSet object that contains the data produced by the query; never null
     * @throws SQLException if a database access error occurs; this method is called on a closed PreparedStatement or the SQL statement does not return a ResultSet object
     */
    @Override
    public ResultSet executeQuery() throws SQLException {
        if (!execute())
            throw new SQLException("Query did not produce a result set", "M1M19");
        return getResultSet();
    }

    /**
     * Override the executeQuery from the Statement to throw a SQLException
     */
    @Override
    public ResultSet executeQuery(final String q) throws SQLException {
        throw new SQLException("This method is not available in a PreparedStatement!", "M1M05");
    }

    /**
     * Executes the SQL statement in this PreparedStatement object, which must be an SQL Data Manipulation Language (DML)
     * statement, such as INSERT, UPDATE or DELETE; or an SQL statement that returns nothing, such as a DDL statement.
     *
     * @return either (1) the row count for SQL Data Manipulation Language (DML) statements or (2) 0 for SQL statements that return nothing
     * @throws SQLException if a database access error occurs; this method is called on a closed PreparedStatement or the SQL statement returns a ResultSet object
     */
    @Override
    public int executeUpdate() throws SQLException {
        if (execute())
            throw new SQLException("Query produced a result set", "M1M17");
        return getUpdateCount();
    }

    /**
     * Override the executeUpdate from the Statement to throw a SQLException
     */
    @Override
    public int executeUpdate(final String q) throws SQLException {
        throw new SQLException("This method is not available in a PreparedStatement!", "M1M05");
    }

    /**
     * Adds a set of parameters to this PreparedStatement object's batch of commands.
     *
     * @throws SQLException if a database access error occurs, this method is called on a closed PreparedStatement
     *                      or an argument is supplied to this method
     */
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

    /**
     * Override the addBatch from the Statement to throw a SQLException
     */
    @Override
    public void addBatch(final String q) throws SQLException {
        throw new SQLException("This method is not available in a PreparedStatement!", "M1M05");
    }

    /**
     * Submits a batch of different parameterized versions of the prepared query to the database for execution.
     * If all commands execute successfully, returns an array of update counts.
     * The int elements of the array that is returned are ordered to correspond to the commands in the batch,
     * which are ordered according to the order in which they were added to the batch.
     * <p>
     * This method overrides Statement's implementation, which batches different queries instead of
     * different parameters for same prepared query.
     *
     * @return an array of update counts containing one element for each command in the batch.
     * The elements of the array are ordered according to the order in which commands were added to the batch.
     * @throws SQLException         if a database access error occurs or this method is called on a closed PreparedStatement
     * @throws BatchUpdateException if one of the commands sent to the database fails to execute properly or attempts to return a result set
     */
    @Override
    public int[] executeBatch() throws SQLException {
        checkNotClosed();
        if (parametersBatch == null || parametersBatch.isEmpty()) {
            return new int[0];
        }
        long[] largeCounts = this.executeLargeBatch();

        //Copy from long[] to int[]
        int[] counts = new int[largeCounts.length];
        for (int i = 0; i < largeCounts.length; i++)
            counts[i] = (largeCounts[i] >= Integer.MAX_VALUE) ? Integer.MAX_VALUE : (int)largeCounts[i];
        return counts;
    }

    /**
     * Submits a batch of different parameterized versions of the prepared query to the database for execution.
     * If all commands execute successfully, returns an array of update counts.
     * The int elements of the array that is returned are ordered to correspond to the commands in the batch,
     * which are ordered according to the order in which they were added to the batch.
     * <p>
     * This method overrides Statement's implementation, which batches different queries instead of
     * different parameters for same prepared query.
     * <p>
     * This method should be used when the returned row count may exceed Integer.MAX_VALUE.
     *
     * @return an array of update counts containing one element for each command in the batch.
     * The elements of the array are ordered according to the order in which commands were added to the batch.
     * @throws SQLException         if a database access error occurs or this method is called on a closed PreparedStatement
     * @throws BatchUpdateException if one of the commands sent to the database fails to execute properly or attempts to return a result set
     */
    public long[] executeLargeBatch() throws SQLException {
        checkNotClosed();
        if (parametersBatch == null || parametersBatch.isEmpty()) {
            return new long[0];
        }
        long[] counts = new long[parametersBatch.size()];
        long count = -1;
        Object[] cur_batch;

        for (int i = 0; i < parametersBatch.size(); i++) {
            //Get batch of parameters
            cur_batch = parametersBatch.get(i);

            if (cur_batch == null) {
                throw new BatchUpdateException();
            }

            for (int j = 0; j < nParams; j++) {
                //Set each parameter in current batch
                setObject(j + 1, cur_batch[j]);
            }

            try {
                count = executeLargeUpdate();
            } catch (SQLException e) {
                //Query returned a resultSet or query failed, throw BatchUpdateException
                throw new BatchUpdateException();
            }
            if (count >= 0) {
                counts[i] = count;
            } else {
                counts[i] = Statement.SUCCESS_NO_INFO;
            }
        }
        clearBatch();
        return counts;
    }

    /**
     * Empties this PreparedStatements object's current list of parameters.
     * <p>
     * This method overrides Statement's implementation, which clears different batched queries instead of
     * different batched parameters for same prepared query.
     *
     * @throws SQLException if this method is called on a closed PreparedStatement
     */
    @Override
    public void clearBatch() throws SQLException {
        checkNotClosed();
        parametersBatch = null;
    }

    /**
     * Retrieves a ResultSetMetaData object that contains information about the columns of the ResultSet object that
     * will be returned when this PreparedStatement object is executed.
     *
     * Because a PreparedStatement object is precompiled, it is possible to know about the ResultSet object that it
     * will return without having to execute it. Consequently, it is possible to invoke the method getMetaData on a
     * PreparedStatement object rather than waiting to execute it and then invoking the ResultSet.getMetaData method on
     * the ResultSet object that is returned.
     *
     * @return the description of a ResultSet object's columns or null if there are no output columns
     * @throws SQLException if a database access error occurs
     */
    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        checkNotClosed();
        return this.resultSetMetaData;
    }

    /**
     * Retrieves the number, types and properties of this PreparedStatement object's parameters.
     *
     * @return a ParameterMetaData object that contains information about the number, types and properties
     * for each parameter marker of this PreparedStatement object
     * @throws SQLException if this method is called on a closed PreparedStatement
     */
    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        checkNotClosed();
        return this.parameterMetaData;
    }

    /**
     * Clears the current parameter values immediately.
     *
     * @throws SQLException if this method is called on a closed PreparedStatement
     */
    //TODO Implement this only on the Java layer?
    @Override
    public void clearParameters() throws SQLException {
        throw new SQLFeatureNotSupportedException("clearParameters()");
        /*checkNotClosed();
        parameters = new Object[nParams];
        MonetNative.monetdbe_clear_bindings(conn.dbNative,statementNative);*/
    }

    /**
     * Sets the value of the designated parameter with the given object.
     * The given Java object will be converted to the given targetSqlType before being sent to the database.
     *
     * @param parameterIndex Parameter index (starts at 1)
     * @param x              the object containing the input parameter value
     * @param targetSqlType  the SQL type (as defined in java.sql.Types) to be sent to the database
     * @param scaleOrLength  for java.sql.Types.DECIMAL or java.sql.Types.NUMERIC types, this is the number of digits after the decimal point
     * @throws SQLException if parameterIndex does not correspond to a parameter marker in the SQL statement;
     *                      if a database access error occurs or this method is called on a closed PreparedStatement
     */
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
            setString(parameterIndex, String.valueOf(x));
        } else if (x instanceof BigDecimal ||
                x instanceof Byte ||
                x instanceof Short ||
                x instanceof Integer ||
                x instanceof Long ||
                x instanceof Float ||
                x instanceof Double) {
            Number num = (Number) x;
            setObjectNum(parameterIndex, targetSqlType, num, x, scaleOrLength);
        } else if (x instanceof Boolean) {
            boolean bool = (Boolean) x;
            setObjectBool(parameterIndex, targetSqlType, bool);
        } else if (x instanceof BigInteger) {
            BigInteger num = (BigInteger) x;
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
        } else if (x instanceof byte[]) {
            switch (targetSqlType) {
                case Types.BINARY:
                case Types.VARBINARY:
                case Types.LONGVARBINARY:
                    setBytes(parameterIndex, (byte[]) x);
                    break;
                default:
                    throw new SQLException("Conversion not allowed", "M1M05");
            }
        } else if (x instanceof java.sql.Date ||
                x instanceof Timestamp ||
                x instanceof Time ||
                x instanceof Calendar ||
                x instanceof java.util.Date ||
                x instanceof java.time.LocalDate ||
                x instanceof java.time.LocalTime ||
                x instanceof java.time.LocalDateTime) {
            setObjectDate(parameterIndex, targetSqlType, x);
        } else if (x instanceof MonetBlob || x instanceof Blob) {
            setBlob(parameterIndex, (Blob) x);
        } else if (x instanceof java.net.URL) {
            setURL(parameterIndex, (URL) x);
        }
    }

    /**
     * Sets the value of the designated parameter from a bool value.
     *
     * @param parameterIndex Parameter index (starts at 1)
     * @param sqlType        the SQL type (as defined in java.sql.Types) of the value to set
     * @param bool           value to be set
     * @throws SQLException if the conversion is not allowed or the BigDecimal object could not be created from the bool
     */
    private void setObjectBool(int parameterIndex, int sqlType, Boolean bool) throws SQLException {
        switch (sqlType) {
            case Types.TINYINT:
                setByte(parameterIndex, (byte) (bool ? 1 : 0));
                break;
            case Types.SMALLINT:
                setShort(parameterIndex, (short) (bool ? 1 : 0));
                break;
            case Types.INTEGER:
                setInt(parameterIndex, (bool ? 1 : 0));  // do not cast to (int) as it generates a compiler warning
                break;
            case Types.BIGINT:
                setLong(parameterIndex, (long) (bool ? 1 : 0));
                break;
            case Types.REAL:
            case Types.FLOAT:
                setFloat(parameterIndex, (float) (bool ? 1.0 : 0.0));
                break;
            case Types.DOUBLE:
                setDouble(parameterIndex, (bool ? 1.0 : 0.0));  // do no cast to (double) as it generates a compiler warning
                break;
            case Types.DECIMAL:
            case Types.NUMERIC: {
                final BigDecimal dec;
                try {
                    dec = new BigDecimal(bool ? 1.0 : 0.0);
                } catch (NumberFormatException e) {
                    throw new SQLException("Internal error: unable to create template BigDecimal: " + e.getMessage(), "M0M03");
                }
                setBigDecimal(parameterIndex, dec);
            }
            break;
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

    /**
     * Sets the value of the designated parameter from a number type (Byte, Short, Integer, Float, Double, BigDecimal).
     *
     * @param parameterIndex Parameter index (starts at 1)
     * @param sqlType        the SQL type (as defined in java.sql.Types) of the value to set
     * @param num            value to be set, as a Number object
     * @param x              value to be set, non-cast
     * @param scale          scale for Decimal and Numeric types
     * @throws SQLException if the conversion is not allowed
     */
    private void setObjectNum(int parameterIndex, int sqlType, Number num, Object x, int scale) throws SQLException {
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
                    } else {
                        setBigDecimal(parameterIndex, new BigDecimal(num.doubleValue()).setScale(scale, java.math.RoundingMode.HALF_UP));
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

    /**
     * Sets the value of the designated parameter from a date type (Date, Time, Timestamp).
     *
     * @param parameterIndex Parameter index (starts at 1)
     * @param targetSqlType  the SQL type (as defined in java.sql.Types) of the value to set
     * @param x              value to be set
     * @throws SQLException if the conversion is not allowed
     */
    private void setObjectDate(int parameterIndex, int targetSqlType, Object x) throws SQLException {
        switch (targetSqlType) {
            case Types.DATE:
                if (x instanceof java.sql.Date) {
                    setDate(parameterIndex, (java.sql.Date) x);
                } else if (x instanceof Timestamp) {
                    setDate(parameterIndex, new java.sql.Date(((Timestamp) x).getTime()));
                } else if (x instanceof java.util.Date) {
                    setDate(parameterIndex, new java.sql.Date(
                            ((java.util.Date) x).getTime()));
                } else if (x instanceof Calendar) {
                    setDate(parameterIndex, new java.sql.Date(
                            ((Calendar) x).getTimeInMillis()));
                } else if (x instanceof LocalDate) {
                    setDate(parameterIndex, Date.valueOf((LocalDate) x));
                } else {
                    throw new SQLException("Conversion not allowed", "M1M05");
                }
                break;
            case Types.TIME:
                if (x instanceof Time) {
                    setTime(parameterIndex, (Time) x);
                } else if (x instanceof Timestamp) {
                    setTime(parameterIndex, new Time(((Timestamp) x).getTime()));
                } else if (x instanceof java.util.Date) {
                    setTime(parameterIndex, new java.sql.Time(
                            ((java.util.Date) x).getTime()));
                } else if (x instanceof Calendar) {
                    setTime(parameterIndex, new java.sql.Time(
                            ((Calendar) x).getTimeInMillis()));
                } else if (x instanceof LocalTime) {
                    setTime(parameterIndex, Time.valueOf((LocalTime) x));
                } else {
                    throw new SQLException("Conversion not allowed", "M1M05");
                }
                break;
            case Types.TIMESTAMP:
                if (x instanceof Timestamp) {
                    setTimestamp(parameterIndex, (Timestamp) x);
                } else if (x instanceof java.sql.Date) {
                    setTimestamp(parameterIndex, new java.sql.Timestamp(((java.sql.Date) x).getTime()));
                } else if (x instanceof java.util.Date) {
                    setTimestamp(parameterIndex, new java.sql.Timestamp(
                            ((java.util.Date) x).getTime()));
                } else if (x instanceof Calendar) {
                    setTimestamp(parameterIndex, new java.sql.Timestamp(
                            ((Calendar) x).getTimeInMillis()));
                } else if (x instanceof LocalDateTime) {
                    setTimestamp(parameterIndex, Timestamp.valueOf((LocalDateTime) x));
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

    /**
     * Sets the value of the designated parameter with the given object.
     * The given Java object will be converted to the given targetSqlType before being sent to the database.
     *
     * @param parameterIndex Parameter index (starts at 1)
     * @param x              the object containing the input parameter value
     * @param targetSqlType  the SQL type to be sent to the database
     * @param scaleOrLength  for java.sql.Types.DECIMAL or java.sql.Types.NUMERIC types, this is the number of digits after the decimal point
     * @throws SQLException if parameterIndex does not correspond to a parameter marker in the SQL statement;
     *                      if a database access error occurs or this method is called on a closed PreparedStatement
     */
    @Override
    public void setObject(int parameterIndex, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        setObject(parameterIndex, x, MonetTypes.getSQLIntFromSQLName(targetSqlType.getName()), scaleOrLength);
    }

    /**
     * Sets the value of the designated parameter with the given object.
     * The given Java object will be converted to the given targetSqlType before being sent to the database.
     *
     * @param parameterIndex Parameter index (starts at 1)
     * @param x              the object containing the input parameter value
     * @param targetSqlType  the SQL type to be sent to the database
     *                       if a database access error occurs or this method is called on a closed PreparedStatement
     */
    @Override
    public void setObject(int parameterIndex, Object x, SQLType targetSqlType) throws SQLException {
        setObject(parameterIndex, x, targetSqlType, 0);
    }

    /**
     * Sets the value of the designated parameter with the given object.
     * This method is similar to setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength),
     * except that it assumes a scale of zero.
     *
     * @param parameterIndex Parameter index (starts at 1)
     * @param x              the object containing the input parameter value
     * @param targetSqlType  the SQL type (as defined in java.sql.Types) to be sent to the database
     * @throws SQLException if parameterIndex does not correspond to a parameter marker in the SQL statement;
     *                      if a database access error occurs or this method is called on a closed PreparedStatement
     */
    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        setObject(parameterIndex, x, targetSqlType, 0);
    }

    /**
     * Sets the value of the designated parameter using the given object.
     * <p>
     * The JDBC specification specifies a standard mapping from Java Object types to SQL types.
     * The given argument will be converted to the corresponding SQL type before being sent to the database.
     *
     * @param parameterIndex Parameter index (starts at 1)
     * @param x              the object containing the input parameter value
     * @throws SQLException if parameterIndex does not correspond to a parameter marker in the SQL statement;
     *                      if a database access error occurs or this method is called on a closed PreparedStatement
     */
    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        checkNotClosed();
        int targetSqlType = MonetTypes.getSQLTypeFromMonet(monetdbeTypes[parameterIndex - 1]);
        setObject(parameterIndex, x, targetSqlType, 0);
    }

    /**
     * Sets the designated parameter to SQL NULL.
     *
     * @param parameterIndex Parameter index (starts at 1)
     * @param sqlType        the SQL type code defined in java.sql.Types (not used)
     * @throws SQLException                    if parameterIndex does not correspond to a parameter marker in the SQL statement;
     *                                         if a database access error occurs or this method is called on a closed PreparedStatement
     */
    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        checkNotClosed();
        if (parameterIndex <= 0 || parameterIndex > nParams)
            throw new SQLException("parameterIndex does not correspond to a parameter marker in the statement");
        int monettype = monetdbeTypes[parameterIndex - 1];

        String error_msg = MonetNative.monetdbe_bind_null(conn.getDbNative(), monettype, statementNative, parameterIndex - 1);
        if (error_msg != null) {
            throw new SQLException(error_msg);
        }
        parameters[parameterIndex - 1] = null;
    }

    /**
     * Sets the designated parameter to the given Java boolean value.
     *
     * @param parameterIndex Parameter index (starts at 1)
     * @param x              the parameter value
     * @throws SQLException if parameterIndex does not correspond to a parameter marker in the SQL statement;
     *                      if a database access error occurs or this method is called on a closed PreparedStatement
     */
    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        checkNotClosed();
        if (parameterIndex <= 0 || parameterIndex > nParams)
            throw new SQLException("parameterIndex does not correspond to a parameter marker in the statement");
        String error_msg = MonetNative.monetdbe_bind_bool(statementNative, parameterIndex - 1, x);
        if (error_msg != null) {
            throw new SQLException(error_msg);
        }
        parameters[parameterIndex - 1] = x;
    }

    /**
     * Sets the designated parameter to the given Java byte value.
     *
     * @param parameterIndex Parameter index (starts at 1)
     * @param x              the parameter value
     * @throws SQLException if parameterIndex does not correspond to a parameter marker in the SQL statement;
     *                      if a database access error occurs or this method is called on a closed PreparedStatement
     */
    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        checkNotClosed();
        if (parameterIndex <= 0 || parameterIndex > nParams)
            throw new SQLException("parameterIndex does not correspond to a parameter marker in the statement");
        String error_msg = MonetNative.monetdbe_bind_byte(statementNative, parameterIndex - 1, x);
        if (error_msg != null) {
            throw new SQLException(error_msg);
        }
        parameters[parameterIndex - 1] = x;
    }

    /**
     * Sets the designated parameter to the given Java short value.
     *
     * @param parameterIndex Parameter index (starts at 1)
     * @param x              the parameter value
     * @throws SQLException if parameterIndex does not correspond to a parameter marker in the SQL statement;
     *                      if a database access error occurs or this method is called on a closed PreparedStatement
     */
    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        checkNotClosed();
        if (parameterIndex <= 0 || parameterIndex > nParams)
            throw new SQLException("parameterIndex does not correspond to a parameter marker in the statement");
        String error_msg = MonetNative.monetdbe_bind_short(statementNative, parameterIndex - 1, x);
        if (error_msg != null) {
            throw new SQLException(error_msg);
        }
        parameters[parameterIndex - 1] = x;
    }

    /**
     * Sets the designated parameter to the given Java int value.
     *
     * @param parameterIndex Parameter index (starts at 1)
     * @param x              the parameter value
     * @throws SQLException if parameterIndex does not correspond to a parameter marker in the SQL statement;
     *                      if a database access error occurs or this method is called on a closed PreparedStatement
     */
    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        checkNotClosed();
        if (parameterIndex <= 0 || parameterIndex > nParams)
            throw new SQLException("parameterIndex does not correspond to a parameter marker in the statement");
        String error_msg = MonetNative.monetdbe_bind_int(statementNative, parameterIndex - 1, x);
        if (error_msg != null) {
            throw new SQLException(error_msg);
        }
        parameters[parameterIndex - 1] = x;
    }

    /**
     * Sets the designated parameter to the given Java long value.
     *
     * @param parameterIndex Parameter index (starts at 1)
     * @param x              the parameter value
     * @throws SQLException if parameterIndex does not correspond to a parameter marker in the SQL statement;
     *                      if a database access error occurs or this method is called on a closed PreparedStatement
     */
    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        checkNotClosed();
        if (parameterIndex <= 0 || parameterIndex > nParams)
            throw new SQLException("parameterIndex does not correspond to a parameter marker in the statement");
        String error_msg = MonetNative.monetdbe_bind_long(statementNative, parameterIndex - 1, x);
        if (error_msg != null) {
            throw new SQLException(error_msg);
        }
        parameters[parameterIndex - 1] = x;
    }

    /**
     * Sets the designated parameter to the given Java float value.
     *
     * @param parameterIndex Parameter index (starts at 1)
     * @param x              the parameter value
     * @throws SQLException if parameterIndex does not correspond to a parameter marker in the SQL statement;
     *                      if a database access error occurs or this method is called on a closed PreparedStatement
     */
    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        checkNotClosed();
        if (parameterIndex <= 0 || parameterIndex > nParams)
            throw new SQLException("parameterIndex does not correspond to a parameter marker in the statement");
        String error_msg = MonetNative.monetdbe_bind_float(statementNative, parameterIndex - 1, x);
        if (error_msg != null) {
            throw new SQLException(error_msg);
        }
        parameters[parameterIndex - 1] = x;
    }

    /**
     * Sets the designated parameter to the given Java double value.
     *
     * @param parameterIndex Parameter index (starts at 1)
     * @param x              the parameter value
     * @throws SQLException if parameterIndex does not correspond to a parameter marker in the SQL statement;
     *                      if a database access error occurs or this method is called on a closed PreparedStatement
     */
    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        checkNotClosed();
        if (parameterIndex <= 0 || parameterIndex > nParams)
            throw new SQLException("parameterIndex does not correspond to a parameter marker in the statement");
        String error_msg = MonetNative.monetdbe_bind_double(statementNative, parameterIndex - 1, x);
        if (error_msg != null) {
            throw new SQLException(error_msg);
        }
        parameters[parameterIndex - 1] = x;
    }

    /**
     * Sets the designated parameter to the given Java java.math.BigDecimal value.
     * Feature not currently supported.
     *
     * @param parameterIndex Parameter index (starts at 1)
     * @param x              the parameter value
     * @throws SQLException if parameterIndex does not correspond to a parameter marker in the SQL statement;
     *                      if a database access error occurs or this method is called on a closed PreparedStatement
     * @throws SQLFeatureNotSupportedException - This feature is not supported
     */
    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        //TODO Implement the C function to bind
        throw new SQLFeatureNotSupportedException("setBigDecimal(int, BigDecimal)");
        /*checkNotClosed();
        if (parameterIndex <= 0 || parameterIndex > nParams)
            throw new SQLException("parameterIndex does not correspond to a parameter marker in the statement");
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
            numberBind = unscaled;
            type = 5;
        }
        MonetNative.monetdbe_bind_decimal(statementNative,numberBind,type,x.scale(),parameterIndex-1);
        parameters[parameterIndex-1] = x;*/
    }

    /**
     * Sets the designated parameter to the given Java java.math.BigInteger value.
     * Feature not currently supported.
     *
     * @param parameterIndex Parameter index (starts at 1)
     * @param x              the parameter value
     * @throws SQLException if parameterIndex does not correspond to a parameter marker in the SQL statement;
     *                      if a database access error occurs or this method is called on a closed PreparedStatement
     * @throws SQLFeatureNotSupportedException - This feature is not supported
     */
    public void setBigInteger(int parameterIndex, BigInteger x) throws SQLException {
        //TODO Implement the C function to bind
        throw new SQLFeatureNotSupportedException("setBigInteger(int, BigInteger)");
        /*checkNotClosed();
        if (parameterIndex <= 0 || parameterIndex > nParams)
            throw new SQLException("parameterIndex does not correspond to a parameter marker in the statement");
        String error_msg = MonetNative.monetdbe_bind_hugeint(statementNative,parameterIndex-1,x);
        if (error_msg != null) {
            throw new SQLException(error_msg);
        }
        parameters[parameterIndex-1] = x;*/
    }

    /**
     * Sets the designated parameter to the given Java String value.
     *
     * @param parameterIndex Parameter index (starts at 1)
     * @param x              the parameter value
     * @throws SQLException if parameterIndex does not correspond to a parameter marker in the SQL statement;
     *                      if a database access error occurs or this method is called on a closed PreparedStatement
     */
    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        checkNotClosed();
        if (parameterIndex <= 0 || parameterIndex > nParams)
            throw new SQLException("parameterIndex does not correspond to a parameter marker in the statement");
        String error_msg = MonetNative.monetdbe_bind_string(statementNative, parameterIndex - 1, x);
        if (error_msg != null) {
            throw new SQLException(error_msg);
        }
        parameters[parameterIndex - 1] = x;
    }

    /**
     * Sets the designated parameter to the given java.sql.Date value, using the given Calendar object.
     * The driver uses the Calendar object to calculate the date taking into account a custom timezone.
     *
     * @param parameterIndex Parameter index (starts at 1)
     * @param x              the parameter value
     * @param cal            the Calendar object the driver will use to construct the date
     * @throws SQLException if parameterIndex does not correspond to a parameter marker in the SQL statement;
     *                      if a database access error occurs or this method is called on a closed PreparedStatement
     */
    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        checkNotClosed();
        if (parameterIndex <= 0 || parameterIndex > nParams)
            throw new SQLException("parameterIndex does not correspond to a parameter marker in the statement");
        LocalDate localDate = x.toLocalDate();

        //Set timezone if there is one
        if (cal != null && localDate != null) {
            localDate = LocalDateTime.of(localDate, LocalTime.now())
                    .atZone(cal.getTimeZone().toZoneId())
                    .withZoneSameInstant(ZoneOffset.UTC)
                    .toLocalDate();
        }
        String error_msg = MonetNative.monetdbe_bind_date(statementNative, parameterIndex - 1, (short) localDate.getYear(), (byte) localDate.getMonthValue(), (byte) localDate.getDayOfMonth());
        if (error_msg != null) {
            throw new SQLException(error_msg);
        }
        parameters[parameterIndex - 1] = x;
    }

    /**
     * Sets the designated parameter to the given java.sql.Time value, using the given Calendar object.
     * The driver uses the Calendar object to calculate the date taking into account a custom timezone.
     *
     * @param parameterIndex Parameter index (starts at 1)
     * @param x              the parameter value
     * @param cal            the Calendar object the driver will use to construct the date
     * @throws SQLException if parameterIndex does not correspond to a parameter marker in the SQL statement;
     *                      if a database access error occurs or this method is called on a closed PreparedStatement
     */
    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        checkNotClosed();
        if (parameterIndex <= 0 || parameterIndex > nParams)
            throw new SQLException("parameterIndex does not correspond to a parameter marker in the statement");
        LocalTime localTime = x.toLocalTime();

        //Set timezone if there is one
        if (cal != null && localTime != null) {
            localTime = LocalDateTime.of(LocalDate.now(), localTime)
                    .atZone(cal.getTimeZone().toZoneId())
                    .withZoneSameInstant(ZoneOffset.UTC)
                    .toLocalTime();
        }

        String error_msg = MonetNative.monetdbe_bind_time(statementNative, parameterIndex - 1, localTime.getHour(), localTime.getMinute(), localTime.getSecond(), localTime.getNano() * 1000);
        if (error_msg != null) {
            throw new SQLException(error_msg);
        }
        parameters[parameterIndex - 1] = x;
    }

    /**
     * Sets the designated parameter to the given java.sql.Timestamp value, using the given Calendar object.
     * The driver uses the Calendar object to calculate the date taking into account a custom timezone.
     *
     * @param parameterIndex Parameter index (starts at 1)
     * @param x              the parameter value
     * @param cal            the Calendar object the driver will use to construct the date
     * @throws SQLException if parameterIndex does not correspond to a parameter marker in the SQL statement;
     *                      if a database access error occurs or this method is called on a closed PreparedStatement
     */
    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        checkNotClosed();
        if (parameterIndex <= 0 || parameterIndex > nParams)
            throw new SQLException("parameterIndex does not correspond to a parameter marker in the statement");
        LocalDateTime localDateTime = x.toLocalDateTime();

        //Set timezone if there is one
        if (cal != null && localDateTime != null) {
            localDateTime = localDateTime.atZone(cal.getTimeZone().toZoneId())
                    .withZoneSameInstant(ZoneOffset.UTC)
                    .toLocalDateTime();
        }
        String error_msg = MonetNative.monetdbe_bind_timestamp(statementNative, parameterIndex - 1, localDateTime.getYear(), localDateTime.getMonthValue(), localDateTime.getDayOfMonth(), localDateTime.getHour(), localDateTime.getMinute(), localDateTime.getSecond(), localDateTime.getNano() * 1000);
        if (error_msg != null) {
            throw new SQLException(error_msg);
        }
        parameters[parameterIndex - 1] = x;
    }

    /**
     * Sets the designated parameter to the given Java java.sql.Date value.
     *
     * @param parameterIndex Parameter index (starts at 1)
     * @param x              the parameter value
     * @throws SQLException if parameterIndex does not correspond to a parameter marker in the SQL statement;
     *                      if a database access error occurs or this method is called on a closed PreparedStatement
     */
    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        setDate(parameterIndex, x, null);
    }

    /**
     * Sets the designated parameter to the given Java java.sql.Time value.
     *
     * @param parameterIndex Parameter index (starts at 1)
     * @param x              the parameter value
     * @throws SQLException if parameterIndex does not correspond to a parameter marker in the SQL statement;
     *                      if a database access error occurs or this method is called on a closed PreparedStatement
     */
    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        setTime(parameterIndex, x, null);
    }

    /**
     * Sets the designated parameter to the given Java java.sql.Timestamp value.
     *
     * @param parameterIndex Parameter index (starts at 1)
     * @param x              the parameter value
     * @throws SQLException if parameterIndex does not correspond to a parameter marker in the SQL statement;
     *                      if a database access error occurs or this method is called on a closed PreparedStatement
     */
    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        setTimestamp(parameterIndex, x, null);
    }

    /**
     * Sets the designated parameter to the given Java byte[] value.
     *
     * @param parameterIndex Parameter index (starts at 1)
     * @param x              the parameter value
     * @throws SQLException if parameterIndex does not correspond to a parameter marker in the SQL statement;
     *                      if a database access error occurs or this method is called on a closed PreparedStatement
     */
    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        checkNotClosed();
        if (parameterIndex <= 0 || parameterIndex > nParams)
            throw new SQLException("parameterIndex does not correspond to a parameter marker in the statement");
        String error_msg = MonetNative.monetdbe_bind_blob(statementNative, parameterIndex - 1, x, x.length);
        if (error_msg != null) {
            throw new SQLException(error_msg);
        }
        parameters[parameterIndex - 1] = x;
    }

    /**
     * Sets the designated parameter to the given Java java.net.URL value.
     *
     * @param parameterIndex Parameter index (starts at 1)
     * @param x              the parameter value
     * @throws SQLException if parameterIndex does not correspond to a parameter marker in the SQL statement;
     *                      if a database access error occurs or this method is called on a closed PreparedStatement
     */
    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        checkNotClosed();
        if (parameterIndex <= 0 || parameterIndex > nParams)
            throw new SQLException("parameterIndex does not correspond to a parameter marker in the statement");
        setString(parameterIndex, x.toString());
        parameters[parameterIndex - 1] = x;
    }

    /**
     * Sets the designated parameter to the given Java java.sql.Blob value.
     *
     * @param parameterIndex Parameter index (starts at 1)
     * @param x              the parameter value
     * @throws SQLException if parameterIndex does not correspond to a parameter marker in the SQL statement;
     *                      if a database access error occurs or this method is called on a closed PreparedStatement
     */
    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        checkNotClosed();
        if (parameterIndex <= 0 || parameterIndex > nParams)
            throw new SQLException("parameterIndex does not correspond to a parameter marker in the statement");

        if (x == null || x.length() <= 0) {
            setNull(parameterIndex, Types.BLOB);
        } else {
            byte[] blob_data = x.getBytes(1, (int) x.length());
            String error_msg = MonetNative.monetdbe_bind_blob(statementNative, parameterIndex - 1, blob_data, x.length());
            if (error_msg != null) {
                throw new SQLException(error_msg);
            }
        }
        parameters[parameterIndex - 1] = x;
    }

    /**
     * Sets the designated parameter with InputStream object which is sent to the server as a BLOB.
     *
     * @param parameterIndex Parameter index (starts at 1)
     * @param inputStream   An object that contains the data to set the parameter value to
     * @throws SQLException if parameterIndex does not correspond to a parameter marker in the SQL statement;
     *                      if a database access error occurs or this method is called on a closed PreparedStatement
     */
    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        checkNotClosed();
        if (parameterIndex <= 0 || parameterIndex > nParams)
            throw new SQLException("parameterIndex does not correspond to a parameter marker in the statement");

        MonetBlob x = new MonetBlob(inputStream);
        setBlob(parameterIndex,x);
    }

    /**
     * Sets the designated parameter with InputStream object which is sent to the server as a BLOB.
     *
     * @param parameterIndex Parameter index (starts at 1)
     * @param inputStream    An object that contains the data to set the parameter value to
     * @param length        The number of bytes in the parameter data
     * @throws SQLException if parameterIndex does not correspond to a parameter marker in the SQL statement;
     *                      if a database access error occurs or this method is called on a closed PreparedStatement;
     *                      if the length specified is less than zero
     */
    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        checkNotClosed();
        if (parameterIndex <= 0 || parameterIndex > nParams)
            throw new SQLException("parameterIndex does not correspond to a parameter marker in the statement");
        if (length < 0)
            throw new SQLException("length cannot be less than zero");

        MonetBlob x = new MonetBlob(inputStream,(int) length);
        setBlob(parameterIndex,x);
    }

    /**
     * Sets the designated parameter to the given Java java.sql.Clob value.
     *
     * @param parameterIndex Parameter index (starts at 1)
     * @param x              the parameter value
     * @throws SQLException if parameterIndex does not correspond to a parameter marker in the SQL statement;
     *                      if a database access error occurs or this method is called on a closed PreparedStatement
     */
    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        checkNotClosed();
        if (parameterIndex <= 0 || parameterIndex > nParams)
            throw new SQLException("parameterIndex does not correspond to a parameter marker in the statement");
        long size = x.length();
        if (size > 0) {
            String error_msg = MonetNative.monetdbe_bind_string(statementNative, parameterIndex - 1, x.toString());
            if (error_msg != null) {
                throw new SQLException(error_msg);
            }
            parameters[parameterIndex - 1] = x;
        } else {
            setNull(parameterIndex, Types.BLOB);
        }
    }

    //Imported from default driver implementation
    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        if (reader == null) {
            setNull(parameterIndex, -1);
            return;
        }

        // Some buffer. Size of 8192 is default for BufferedReader, so...
        final int size = 8192;
        final char[] arr = new char[size];
        final StringBuilder buf = new StringBuilder(size * 32);
        try {
            int numChars;
            while ((numChars = reader.read(arr, 0, size)) > 0) {
                buf.append(arr, 0, numChars);
            }
            setString(parameterIndex, buf.toString());
        } catch (IOException e) {
            throw new SQLException("failed to read from stream: " + e.getMessage(), "M1M25");
        }
    }

    //Imported from default driver implementation
    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        if (reader == null) {
            setNull(parameterIndex, -1);
            return;
        }
        if (length < 0 || length > Integer.MAX_VALUE) {
            throw new SQLException("Invalid length value: " + length, "M1M05");
        }

        // simply serialise the Reader data into a large buffer
        final CharBuffer buf = CharBuffer.allocate((int) length); // have to down cast
        try {
            reader.read(buf);
            // We have to rewind the buffer, because otherwise toString() returns "".
            buf.rewind();
            setString(parameterIndex, buf.toString());
        } catch (IOException e) {
            throw new SQLException("failed to read from stream: " + e.getMessage(), "M1M25");
        }
    }

    /**
     * Sets the designated parameter to SQL NULL.
     * This version of the method setNull is supposed to be used for user-defined types and REF type parameters.
     * Because MonetDBe currently doesn't support UDFs, this behaves similarly to setNull(int parameterIndex, int sqlType).
     *
     * @see #setNull(int, int)
     */
    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        //Ignore typeName parameter, no support for Ref and UDFs in monetdbe
        setNull(parameterIndex, sqlType);
    }

    /**
     * Similar to setString(int,String).
     *
     * @see #setString(int, String)
     */
    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        setString(parameterIndex, value);
    }

    /**
     * Similar to setClob(int,Reader,long).
     *
     * @see #setClob(int, Reader, long)
     */
    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        setClob(parameterIndex, reader, (long) length);
    }

    /**
     * Similar to setClob(int,Reader,long).
     *
     * @see #setClob(int, Reader, long)
     */
    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        setClob(parameterIndex, reader, length);
    }

    /**
     * Similar to setClob(int,Reader).
     *
     * @see #setClob(int, Reader)
     */
    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        setClob(parameterIndex, reader);
    }

    /**
     * Similar to setClob(int,Reader).
     *
     * @see #setClob(int, Reader)
     */
    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        setClob(parameterIndex, value);
    }

    /**
     * Similar to setClob(int,Reader,long).
     *
     * @see #setClob(int, Reader, long)
     */
    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        setClob(parameterIndex, value, length);
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("setAsciiStream");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("setAsciiStream");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("setAsciiStream");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("setUnicodeStream");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("setBinaryStream");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("setBinaryStream");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("setBinaryStream");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException("setRef");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException("setSQLXML");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException("setArray");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException("setRowId");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        throw new SQLFeatureNotSupportedException("setNClob");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("setNClob");
    }

    /**
     * Feature not supported.
     *
     * @throws SQLFeatureNotSupportedException This feature is not supported
     */
    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("setNClob");
    }
}
