package org.monetdb.monetdbe;

import java.nio.ByteBuffer;
import java.sql.*;
import java.util.*;
import java.util.concurrent.Executor;
/**
 * A {@link Connection} suitable for the MonetDB database.
 *
 * This connection represents a connection (session) to a MonetDB
 * database. SQL statements are executed and results are returned within
 * the context of a connection. This Connection object holds a physical
 * connection to the MonetDB database.
 *
 * A Connection object's database should able to provide information
 * describing its tables, its supported SQL grammar, its stored
 * procedures, the capabilities of this connection, and so on. This
 * information is obtained with the getMetaData method.
 *
 * Note: By default a Connection object is in auto-commit mode, which
 * means that it automatically commits changes after executing each
 * statement. If auto-commit mode has been disabled, the method commit
 * must be called explicitly in order to commit changes; otherwise,
 * database changes will not be saved.
 */
public class MonetConnection extends MonetWrapper implements Connection {
    /** The pointer to the C database object */
    protected ByteBuffer dbNative;
    /** The property options for this Connection object */
    private Properties properties;
    /** The statements created with this Connection object */
    private List<MonetStatement> statements;
    /** The stack of warnings for this Connection object */
    private SQLWarning warnings;
    /** The timeout to gracefully terminate the session */
    private int sessiontimeout;
    /** The timeout to gracefully terminate the query */
    private int querytimeout;
    /** The amount of RAM to be used, in MB */
    private int memorylimit;
    /** Maximum number of worker treads, limits level of parallelism */
    private int nr_threads;
    /** Whether this Connection is in autocommit mode */
    private boolean autoCommit;
    /** The full MonetDB JDBC Connection URL used for this Connection */
    private String jdbcURL;

    /**
     * Constructor of a Connection for MonetDB.
     *
     * @param props a Property hashtable holding the properties needed for connecting
     * @throws SQLException if a database error occurs
     * @throws IllegalArgumentException if one of the required arguments is null or empty
     */
    //TODO Verify input argument format and throw IllegalArgumentException when needed
    MonetConnection(Properties props) throws SQLException, IllegalArgumentException {
        //Set database options
        this.sessiontimeout = Integer.parseInt(props.getProperty("sessiontimeout", "0"));
        this.querytimeout = Integer.parseInt(props.getProperty("querytimeout", "0"));
        this.memorylimit = Integer.parseInt(props.getProperty("memorylimit", "0"));
        this.nr_threads = Integer.parseInt(props.getProperty("nr_threads", "0"));

        //Necessary for DatabaseMetadata method
        this.jdbcURL = props.getProperty("jdbc-url");

        String error_msg;

        //Remote proxy databases
        //TODO Find better condition for figuring out if its remote proxy?
        if (props.containsKey("host") && props.containsKey("database")) {
            String host = props.getProperty("host", "localhost");
            int port = Integer.parseInt(props.getProperty("port", "50000"));
            String database = props.getProperty("database","test");
            String user = props.getProperty("user", "monetdb");
            String password = props.getProperty("password", "monetdb");

            //Remote connections pass a null argument for URL
            error_msg = MonetNative.monetdbe_open(null, this, sessiontimeout, querytimeout, memorylimit, nr_threads, host, port, database, user, password);
        }
        //Local directory and in-memory databases
        else {
            //Directory for local, null for in-memory
            String path = props.getProperty("path", null);
            error_msg = MonetNative.monetdbe_open(path, this, sessiontimeout, querytimeout, memorylimit, nr_threads);
        }

        //Error when opening db
        if (dbNative == null || error_msg != null) {
            throw new SQLException(error_msg);
        }
        this.properties = props;
        this.statements = new ArrayList<>();

        //Auto-commit defaults to true. If the passed property is different, change it
        this.autoCommit = true;
        if (props.containsKey("autocommit") && props.getProperty("autocommit").equals("false"))
            setAutoCommit(false);
    }

    /**
     * Helper method to execute SQL statements within Connection methods.
     * Used in commit(), rollback() and setSchema()
     */
    private void executeCommand(String sql) throws SQLException {
        checkNotClosed();
        MonetStatement st = null;
        try {
            st = (MonetStatement) createStatement();
            if (st != null) {
                st.execute(sql);
            }
        } finally {
            st.close();
            statements.remove(st);
        }
    }

    //Transactions and closing
    /**
     * Makes all changes made since the previous commit/rollback
     * permanent and releases any database locks currently held by this
     * Connection object.  This method should be used only when
     * auto-commit mode has been disabled.
     *
     * @throws SQLException if a database access error occurs
     * @see #setAutoCommit(boolean)
     */
    @Override
    public void commit() throws SQLException {
        checkNotClosed();
        if (getAutoCommit())
            throw new SQLException("COMMIT: not allowed in auto commit mode");
        executeCommand("COMMIT");
    }

    /**
     * Undoes all changes made in the current transaction and releases
     * any database locks currently held by this Connection object.
     * This method should be used only when auto-commit mode has been disabled.
     *
     * @throws SQLException if a database access error occurs
     * @see #setAutoCommit(boolean)
     */
    @Override
    public void rollback() throws SQLException {
        checkNotClosed();
        if (getAutoCommit())
            throw new SQLException("Operation not permitted in autocommit");
        executeCommand("ROLLBACK");
    }

    /**
     * Helper method to test whether the Connection object is closed
     * When closed, it throws an SQLException
     */
    private void checkNotClosed() throws SQLException {
        if (isClosed())
            throw new SQLException("Connection is closed", "M1M20");
    }

    /**
     * Releases this Connection object's database and JDBC resources
     * immediately. All Statements created from this Connection will be
     * closed when this method is called.
     *
     * Calling the method close on a Connection object that is already
     * closed is a no-op.
     *
     * @throws SQLException if a database access error occurs
     */
    @Override
    public void close() throws SQLException {
        checkNotClosed();
        for (MonetStatement s : statements) {
            try {
                s.close();
            } catch (SQLException e) {
                //Statement already closed
            }
        }
        statements = null;
        String error_msg = MonetNative.monetdbe_close(dbNative);
        if (error_msg != null) {
            throw new SQLException(error_msg);
        }
        dbNative = null;
    }

    /**
     * Retrieves whether this Connection object has been closed.
     *
     * This method cannot be called to determine whether a connection
     * to a database is valid or invalid. A typical client can determine that a
     * connection is invalid by using the {@link #isValid(int timeout) isValid()} method.
     *
     * @return true if this Connection object is closed; false if it is still open
     * @throws SQLException if a database access error occurs
     */
    @Override
    public boolean isClosed() throws SQLException {
        return dbNative == null;
    }

    /**
     * Terminates an open connection. The current implementation doesn't
     * use the Executor argument, meaning it is identical to the {@link #close() close()} method.
     *
     * @param executor The Executor implementation which will be used by
     *        abort (not used)
     * @throws SQLException if a database access error occurs or the
     *         executor is null
     */
    @Override
    public void abort(Executor executor) throws SQLException {
        checkNotClosed();
        if (executor == null)
            throw new SQLException("executor is null", "M1M05");
        close();
    }

    /**
     * Returns true if the connection has not been closed and is still
     * valid. The driver will submit a query on the connection to
     * verify the connection is still valid when this method is called.
     * The timeout parameter is not currently used (executes as if it is equal to 0).
     *
     * @param timeout Not currently used. The time in seconds to wait for the database
     *        operation used to validate the connection to complete.
     * @return true if the connection is valid, false otherwise
     * @throws SQLException if the value supplied for timeout is less than 0
     */
    @Override
    public boolean isValid(int timeout) throws SQLException {
        if (timeout < 0)
            throw new SQLException("timeout is less than 0", "M1M05");
        if (isClosed())
            return false;
        Statement st = null;
        ResultSet rs = null;
        boolean isValid = false;
        try {
            st = createStatement();
            if (st != null) {
                rs = st.executeQuery("SELECT 1");
                if (rs != null && rs.next()) {
                    isValid = true;
                }
            }
        } catch (SQLException e) {
        } finally {
            if (rs != null)
                rs.close();
            if (st != null)
                st.close();
        }

        return isValid;
    }


    //Warnings
    /**
     * Retrieves the first warning reported by calls on this Connection
     * object.  If there is more than one warning, subsequent warnings
     * will be chained to the first one and can be retrieved by calling
     * the method SQLWarning.getNextWarning on the warning that was
     * retrieved previously.
     *
     * @return the first SQLWarning object or null if there are none
     * @throws SQLException if a database access error occurs or this method is
     *         called on a closed connection
     */
    @Override
    public SQLWarning getWarnings() throws SQLException {
        checkNotClosed();
        return warnings;
    }

    private void addWarning(final String reason, final String sqlstate) {
        final SQLWarning warn = new SQLWarning(reason, sqlstate);
        if (warnings == null) {
            warnings = warn;
        } else {
            warnings.setNextWarning(warn);
        }
    }

    /**
     * Clears all warnings reported for this Connection object. After a
     * call to this method, the method getWarnings returns null until a
     * new warning is reported for this Connection object.
     *
     * @throws SQLException if a database access error occurs or this method is called on a closed connection
     */
    @Override
    public void clearWarnings() throws SQLException {
        checkNotClosed();
        warnings = null;
    }

    //Sets and gets
    /**
     * Retrieve the C pointer to the database.
     * Used in ResultSet, Statement and PreparedStatement
     */
    protected ByteBuffer getDbNative() {
        return dbNative;
    }

    /**
     * Sets this connection's auto-commit mode to the given state. If a
     * connection is in auto-commit mode, then all its SQL statements
     * will be executed and committed as individual transactions.
     * Otherwise, its SQL statements are grouped into transactions that
     * are terminated by a call to either the method commit or the
     * method rollback. By default, new connections are in auto-commit mode.
     *
     * @param autoCommit true to enable auto-commit mode; false to disable it
     * @throws SQLException if a database access error occurs
     * @see #getAutoCommit()
     */
    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        checkNotClosed();
        if (autoCommit != this.autoCommit) {
            this.autoCommit = autoCommit;
            String error_msg = MonetNative.monetdbe_set_autocommit(dbNative, autoCommit ? 1 : 0);
            if (error_msg != null) {
                throw new SQLException(error_msg);
            }
        }
    }

    /**
     * Retrieves the current auto-commit mode for this Connection object.
     *
     * @return the current state of this Connection object's auto-commit mode
     * @see #setAutoCommit(boolean)
     */
    @Override
    public boolean getAutoCommit() throws SQLException {
        checkNotClosed();
        //Calling the server instead of returning the Java variable because the value may have changed
        return MonetNative.monetdbe_get_autocommit(dbNative);
    }

    /**
     * Retrieves a DatabaseMetaData object that contains metadata about
     * the database to which this Connection object represents a
     * connection. The metadata includes information about the
     * database's tables, its supported SQL grammar, its stored
     * procedures, the capabilities of this connection, and so on.
     *
     * @throws SQLException if the current language is not SQL
     * @return a DatabaseMetaData object for this Connection object
     */
    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        checkNotClosed();
        return new MonetDatabaseMetaData(this);
    }

    /**
     * Puts this connection in read-only mode as a hint to the driver to
     * enable database optimizations.  MonetDB doesn't support read-only mode,
     * hence an SQLWarning is generated if attempted to set to true here.
     *
     * @param readOnly true attempts to turn on read-only mode; false disables it
     * @throws SQLException if a database access error occurs or this
     *         method is called during a transaction.
     */
    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        checkNotClosed();
        if (readOnly) {
            addWarning("cannot setReadOnly(true): read-only Connection mode not supported", "01M08");
        }
    }

    /**
     * Retrieves whether this Connection object is in read-only mode.
     * MonetDB Connection objects are never in read-only mode.
     *
     * @return false, as MonetDB doesn't support read-only mode
     */
    @Override
    public boolean isReadOnly() throws SQLException {
        checkNotClosed();
        return false;
    }

    /**
     * Sets the given catalog name in order to select a subspace of this
     * Connection object's database in which to work.  Because MonetDB
     * does not support catalogs, the driver will silently ignore this request.
     */
    @Override
    public void setCatalog(String catalog) throws SQLException {
        //ignore this request as MonetDB does not support catalogs
    }

    /**
     * Retrieves this Connection object's current catalog name. Because MonetDB
     * does not support catalogs, the driver will return a null.
     *
     * @return the current catalog name or null if there is none
     */
    @Override
    public String getCatalog() throws SQLException {
        // MonetDB does NOT support catalogs
        return null;
    }

    /**
     * Attempts to change the transaction isolation level for this
     * Connection object to the one given. This driver only supports
     * TRANSACTION_SERIALIZABLE, so a warning will be generated if
     * another level is set.
     *
     * @param level one of the following Connection constants:
     *        Connection.TRANSACTION_READ_UNCOMMITTED,
     *        Connection.TRANSACTION_READ_COMMITTED,
     *        Connection.TRANSACTION_REPEATABLE_READ, or
     *        Connection.TRANSACTION_SERIALIZABLE.
     */
    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        checkNotClosed();
        if (level != TRANSACTION_SERIALIZABLE) {
            addWarning("MonetDB only supports fully serializable " +
                    "transactions, continuing with transaction level " +
                    "raised to TRANSACTION_SERIALIZABLE", "01M09");
        }
    }

    /**
     * Retrieves this Connection object's current transaction isolation
     * level.
     *
     * @return the current transaction isolation level, which will be
     *         Connection.TRANSACTION_SERIALIZABLE
     */
    @Override
    public int getTransactionIsolation() throws SQLException {
        checkNotClosed();
        return TRANSACTION_SERIALIZABLE;
    }

    /**
     * Retrieves the Map object associated with this Connection object.
     * Unless the application has added an entry, the type map returned
     * will be empty.
     *
     * Not supported currently.
     *
     * @return the java.util.Map object associated with this Connection
     *         object
     */
    //TODO UDTs
    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        checkNotClosed();
        throw new SQLFeatureNotSupportedException("getTypeMap()");
    }

    /**
     * Installs the given TypeMap object as the type map for this
     * Connection object. The type map will be used for the custom
     * mapping of SQL structured types and distinct types.
     *
     * Not supported currently.
     *
     * @param map the java.util.Map object to install as the replacement for
     *        this Connection  object's default type map
     */
    //TODO UDTs
    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        checkNotClosed();
        throw new SQLFeatureNotSupportedException("setTypeMap(Map<String, Class<?>> map)");
    }

    /**
     * Changes the default holdability of ResultSet objects created using this
     * Connection object to the given holdability.
     *
     * This driver only supports HOLD_CURSORS_OVER_COMMIT, so an exception will
     * be thrown if another one is set
     *
     * @param holdability - a ResultSet holdability constant; one of
     *	ResultSet.HOLD_CURSORS_OVER_COMMIT or
     *	ResultSet.CLOSE_CURSORS_AT_COMMIT
     * @throws SQLFeatureNotSupportedException - if a holdability different from HOLD_CURSORS_OVER_COMMIT is set.
     */
    @Override
    public void setHoldability(int holdability) throws SQLException {
        checkNotClosed();
        if (holdability != ResultSet.HOLD_CURSORS_OVER_COMMIT)
            throw new SQLFeatureNotSupportedException("setHoldability(CLOSE_CURSORS_AT_COMMIT)");
    }

    /**
     * Retrieves the current holdability of ResultSet objects created
     * using this Connection object.
     *
     * @return ResultSet.HOLD_CURSORS_OVER_COMMIT
     */
    @Override
    public int getHoldability() throws SQLException {
        checkNotClosed();
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    /**
     * Sets the value of the client info property specified by name to the value specified by value.
     * Options supported by the driver can be determined by calling
     * {@link Driver#getPropertyInfo(String, Properties)}
     *
     * Options set with this method are currently not used, as changing the options
     * after starting a new connection is not supported. Configurations must be
     * currently set prior to starting a new connection to the database
     *
     * @param name - The name of the client info property to set
     * @param value - The value to set the client info property to. If the
     *        value is null, the current value of the specified property is cleared.
     * @throws SQLClientInfoException - if the database server returns an error
     *         while setting the clientInfo values on the database server
     *         or this method is called on a closed connection
     */
    //TODO Update configurations instead of only changing the properties argument
    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        try {
            checkNotClosed();
        } catch (SQLException e) {
            throw new SQLClientInfoException();
        }
        this.properties.setProperty(name, value);
    }

    /**
     * Sets the value of the connection's client info properties.
     * The Properties object contains the names and values of the client info
     * properties to be set. The set of client info properties contained in the
     * properties list replaces the current set of client info properties on the connection.
     * Options supported by the driver can be determined by calling
     * {@link Driver#getPropertyInfo(String, Properties)}
     *
     * Options set with this method are currently not used, as changing the options
     * after starting a new connection is not supported. Configurations must be
     * currently set prior to starting a new connection to the database
     *
     * @param properties - The list of client info properties to set
     * @throws SQLClientInfoException - if the database server returns an error
     * while setting the clientInfo values on the database server
     * or this method is called on a closed connection
     */
    //TODO Update configurations instead of only changing the properties argument
    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        try {
            checkNotClosed();
        } catch (SQLException e) {
            throw new SQLClientInfoException();
        }
        this.properties = properties;
    }

    /**
     * Returns the value of the client info property specified by name.
     * This method may return null if the specified client info property
     * has not been set and does not have a default value.
     * Options supported by the driver can be determined by calling
     * {@link Driver#getPropertyInfo(String, Properties)}
     *
     * @param name - The name of the client info property to retrieve
     * @return The value of the client info property specified or null
     */
    @Override
    public String getClientInfo(String name) throws SQLException {
        checkNotClosed();
        return properties.getProperty(name);
    }

    /**
     * Returns a list containing the name and current value of each client info
     * property supported by the driver. The value of a client info property may
     * be null if the property has not been set and does not have a default value.
     * Options supported by the driver can be determined by calling
     * {@link Driver#getPropertyInfo(String, Properties)}
     *
     * @return A Properties object that contains the name and current value
     *         of each of the client info properties supported by the driver.
     */
    @Override
    public Properties getClientInfo() throws SQLException {
        checkNotClosed();
        return properties;
    }

    /**
     * Sets the given schema name to access.
     *
     * @param schema the name of a schema in which to work
     * @throws SQLException if a database access error occurs or this
     *         method is called on a closed connection
     */
    @Override
    public void setSchema(String schema) throws SQLException {
        checkNotClosed();
        if (schema == null || schema.isEmpty())
            throw new SQLException("Missing schema name", "M1M05");
        executeCommand("SET SCHEMA \"" + schema + "\"");
    }

    /**
     * Retrieves this Connection object's current schema name.
     *
     * @return the current schema name or null if there is none
     * @throws SQLException if a database access error occurs or this
     *         method is called on a closed connection
     */
    @Override
    public String getSchema() throws SQLException {
        checkNotClosed();
        String cur_schema = null;
        Statement st = null;
        ResultSet rs = null;
        try {
            st = createStatement();
            if (st != null) {
                rs = st.executeQuery("SELECT CURRENT_SCHEMA");
                if (rs != null) {
                    if (rs.next())
                        cur_schema = rs.getString(1);
                }
            }
            // do not catch any Exception, just let it propagate
        } finally {
            rs.close();
            st.close();
        }
        if (cur_schema == null)
            throw new SQLException("Failed to fetch schema name", "02000");
        return cur_schema;
    }

    /**
     * Sets the maximum period a Connection or objects created from the
     * Connection will wait for the database to reply to any one
     * request.
     *
     * This functionality is currently not implemented.
     *
     * @param executor The Executor implementation which will be used by
     *        setNetworkTimeout
     * @param milliseconds The time in milliseconds to wait for the
     *        database operation to complete
     * @throws SQLException if a database access error occurs, this
     *         method is called on a closed connection, the executor is
     *         null, or the value specified for seconds is less than 0.
     */
    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        checkNotClosed();
        if (executor == null || milliseconds < 0)
            throw new SQLException();
        //Using session timeout for now
        this.sessiontimeout = milliseconds;
    }

    /**
     * Retrieves the number of milliseconds the driver will wait for a
     * database request to complete. If the limit is exceeded, a
     * SQLException is thrown.
     *
     * This functionality is currently not implemented.
     *
     * @return the current timeout limit in milliseconds; zero means
     *         there is no limit
     * @throws SQLException if a database access error occurs or
     *         this method is called on a closed Connection
     */
    @Override
    public int getNetworkTimeout() throws SQLException {
        checkNotClosed();
        //Returning session timeout for now
        return this.sessiontimeout;
    }

    /**
     * Converts the given SQL statement into the system's native SQL grammar.
     *
     * This feature is currently not supported
     *
     * @param sql - an SQL statement that may contain one or more '?' parameter placeholders.
     * @return the native form of this statement
     */
    @Override
    public String nativeSQL(final String sql) {
        /* there is currently no way to get the native MonetDB rewritten SQL string back, so just return the original string */
        return sql;
    }

    /**
     * Returns the full JDBC Connection URL used for connecting to the database.
     * It is called from getURL()in MonetDatabaseMetaData.
     * @return the MonetDB JDBC Connection URL
     */
    public String getJdbcURL() {
        return jdbcURL;
    }

    /**
     * Returns the full JDBC Connection URL used for connecting to the database.
     * It is called from getUserName()in MonetDatabaseMetaData.
     * @return the current User Name
     */
    public String getUserName() throws SQLException {
        checkNotClosed();
        String cur_user = null;
        Statement st = null;
        ResultSet rs = null;
        try {
            st = createStatement();
            if (st != null) {
                rs = st.executeQuery("SELECT CURRENT_USER");
                if (rs != null) {
                    if (rs.next())
                        cur_user = rs.getString(1);
                }
            }
            // do not catch any Exception, just let it propagate
        } finally {
            rs.close();
            st.close();
        }
        if (cur_user == null)
            throw new SQLException("Failed to fetch user name");
        return cur_user;
    }

    /**
     * Returns the maximum number of possible active connections.
     * It is called from getMaxConnections()in MonetDatabaseMetaData
     * @return the maximum number of active connections possible at one time;
     * a result of zero means that there is no limit or the limit is not known
     */
    public int getMaxConnections() throws SQLException {
        checkNotClosed();
        int maxConnections = 0;
        Statement st = null;
        ResultSet rs = null;
        try {
            st = createStatement();
            if (st != null) {
                rs = st.executeQuery("SELECT value FROM sys.env() WHERE name = 'max_clients'");
                if (rs != null) {
                    if (rs.next())
                        maxConnections = rs.getInt(1);
                }
            }
            // do not catch any Exception, just let it propagate
        } finally {
            rs.close();
            st.close();
        }
        return maxConnections;
    }

    //Statements
    /**
     * Creates a Statement object for sending SQL statements to the
     * database.  SQL statements without parameters are normally
     * executed using Statement objects. If the same SQL statement is
     * executed many times, it may be more efficient to use a
     * PreparedStatement object.
     *
     * Result sets created using the returned Statement object will by
     * default be type TYPE_SCROLL_INSENSITIVE and have a concurrency level of
     * CONCUR_READ_ONLY and a holdability of HOLD_CURSORS_OVER_COMMIT.
     *
     * @return a new default Statement object
     * @throws SQLException if a database access error occurs
     */
    @Override
    public Statement createStatement() throws SQLException {
        return createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    /**
     * Creates a Statement object that will generate ResultSet objects
     * with the given type and concurrency. This method is the same as
     * the createStatement method above, but it allows the default
     * result set type and concurrency to be overridden.
     *
     * Non-default result set properties are ignored in the current version.
     *
     * @param resultSetType a result set type; one of
     *        ResultSet.TYPE_FORWARD_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE,
     *        or ResultSet.TYPE_SCROLL_SENSITIVE
     * @param resultSetConcurrency a concurrency type; one of
     *        ResultSet.CONCUR_READ_ONLY or ResultSet.CONCUR_UPDATABLE
     * @return a new Statement object that will generate ResultSet objects with
     *         the given type and concurrency
     * @throws SQLException if a database access error occurs
     */
    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return createStatement(resultSetType, resultSetConcurrency, ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    /**
     * Creates a Statement object that will generate ResultSet objects
     * with the given type, concurrency, and holdability.  This method
     * is the same as the createStatement method above, but it allows
     * the default result set type, concurrency, and holdability to be
     * overridden.
     *
     * Non-default result set properties are ignored in the current version.
     *
     * @param resultSetType one of the following ResultSet constants:
     * ResultSet.TYPE_FORWARD_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE,
     * or ResultSet.TYPE_SCROLL_SENSITIVE
     * @param resultSetConcurrency one of the following ResultSet
     * constants: ResultSet.CONCUR_READ_ONLY or
     * ResultSet.CONCUR_UPDATABLE
     * @param resultSetHoldability one of the following ResultSet
     * constants: ResultSet.HOLD_CURSORS_OVER_COMMIT or
     * ResultSet.CLOSE_CURSORS_AT_COMMIT
     *
     * @return a new Statement object that will generate ResultSet
     * objects with the given type, concurrency, and holdability
     * @throws SQLException if a database access error occurs or the
     * given parameters are not ResultSet constants indicating type,
     * concurrency, and holdability
     */
    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        checkNotClosed();
        try {
            MonetStatement s = new MonetStatement(this);
            statements.add(s);
            return s;
        } catch (IllegalArgumentException e) {
            throw new SQLException(e.toString(), "M0M03");
        }
    }

    /**
     * Creates a CallableStatement object for calling database stored procedures.
     * The CallableStatement object provides methods for setting up its IN parameters,
     * and methods for executing the call to a stored procedure.
     *
     * Result sets created using the returned CallableStatement object will by default be type TYPE_SCROLL_INSENSITIVE,
     * have a concurrency level of CONCUR_READ_ONLY and have holdability of HOLD_CURSORS_OVER_COMMIT.
     *
     * @param sql - an SQL statement that may contain one or more '?' parameter placeholders.
     *	Typically this statement is specified using JDBC call escape syntax.
     * @return a new default CallableStatement object containing the pre-compiled SQL statement
     * @throws SQLException - if a database access error occurs or this method is called on a closed connection
     */
    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        return prepareCall(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    /**
     * Creates a CallableStatement object that will generate ResultSet objects with the given type and concurrency.
     * This method is the same as the prepareCall method above, but it allows the default result set type and concurrency to be overridden.
     *
     * Non-default result set properties are ignored in the current version.
     *
     * @param sql - a String object that is the SQL statement to be sent to the database; may contain on or more '?' parameters
     *	Typically this statement is specified using JDBC call escape syntax.
     * @param resultSetType - a result set type; one of ResultSet.TYPE_FORWARD_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE, or ResultSet.TYPE_SCROLL_SENSITIVE
     * @param resultSetConcurrency - a concurrency type; one of ResultSet.CONCUR_READ_ONLY or ResultSet.CONCUR_UPDATABLE
     * @return a new CallableStatement object containing the pre-compiled SQL statement that
     *	will produce ResultSet objects with the given type and concurrency
     * @throws SQLException - if a database access error occurs, this method is called on a closed connection or
     *	the given parameters are not ResultSet constants indicating type and concurrency
     */
    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return prepareCall(sql, resultSetType, resultSetConcurrency, ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    /**
     * Creates a CallableStatement object that will generate ResultSet objects with the given type and concurrency.
     * This method is the same as the prepareCall method above, but it allows the default result set type, result set concurrency type and holdability to be overridden.
     *
     * Non-default result set properties are ignored in the current version.
     *
     * @param sql - a String object that is the SQL statement to be sent to the database; may contain on or more '?' parameters
     *	Typically this statement is specified using JDBC call escape syntax.
     * @param resultSetType - a result set type; one of ResultSet.TYPE_FORWARD_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE, or ResultSet.TYPE_SCROLL_SENSITIVE
     * @param resultSetConcurrency - a concurrency type; one of ResultSet.CONCUR_READ_ONLY or ResultSet.CONCUR_UPDATABLE
     * @param resultSetHoldability - one of the following ResultSet constants: ResultSet.HOLD_CURSORS_OVER_COMMIT or ResultSet.CLOSE_CURSORS_AT_COMMIT
     * @return a new CallableStatement object, containing the pre-compiled SQL statement, that will generate ResultSet objects with the given type, concurrency, and holdability
     * @throws SQLException - if a database access error occurs, this method is called on a closed connection or
     *	the given parameters are not ResultSet constants indicating type, concurrency, and holdability
     */
    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        checkNotClosed();
        try {
            MonetCallableStatement s = new MonetCallableStatement(this, sql);
            statements.add(s);
            return s;
        } catch (IllegalArgumentException e) {
            throw new SQLException(e.toString(), "M0M03");
        }
    }

    /**
     * Creates a PreparedStatement object for sending parameterized SQL
     * statements to the database.
     *
     * A SQL statement with or without IN parameters can be pre-compiled
     * and stored in a PreparedStatement object. This object can then be
     * used to efficiently execute this statement multiple times.
     *
     * Result sets created using the returned PreparedStatement object
     * will by default be type TYPE_SCROLL_INSENSITIVE, have a concurrency level of CONCUR_READ_ONLY
     * and have holdability of HOLD_CURSORS_OVER_COMMIT
     *
     * @param sql an SQL statement that may contain one or more '?' IN
     *        parameter placeholders
     * @return a new default PreparedStatement object containing the
     *         pre-compiled SQL statement
     * @throws SQLException if a database access error occurs
     */
    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    /**
     * Creates a PreparedStatement object that will generate ResultSet
     * objects with the given type and concurrency.  This method is the
     * same as the prepareStatement method above, but it allows the
     * default result set type and concurrency to be overridden.
     *
     * Non-default result set properties are ignored in the current version.
     *
     * @param sql a String object that is the SQL statement to be sent to the
     *        database; may contain one or more ? IN parameters
     * @param resultSetType a result set type; one of
     *        ResultSet.TYPE_FORWARD_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE,
     *        or ResultSet.TYPE_SCROLL_SENSITIVE
     * @param resultSetConcurrency a concurrency type; one of
     *        ResultSet.CONCUR_READ_ONLY or ResultSet.CONCUR_UPDATABLE
     * @return a new PreparedStatement object containing the pre-compiled SQL
     *         statement that will produce ResultSet objects with the given
     *         type and concurrency
     * @throws SQLException if a database access error occurs or the given
     *         parameters are not ResultSet constants indicating
     *         type and concurrency
     */
    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return prepareStatement(sql, resultSetType, resultSetConcurrency, ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    /**
     * Creates a PreparedStatement object that will generate ResultSet
     * objects with the given type, concurrency, and holdability.
     *
     * This method is the same as the prepareStatement method above, but
     * it allows the default result set type, concurrency, and
     * holdability to be overridden.
     *
     * Non-default result set properties are ignored in the current version.
     *
     * @param sql a String object that is the SQL statement to be sent
     * to the database; may contain one or more ? IN parameters
     * @param resultSetType one of the following ResultSet constants:
     * ResultSet.TYPE_FORWARD_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE,
     * or ResultSet.TYPE_SCROLL_SENSITIVE
     * @param resultSetConcurrency one of the following ResultSet
     * constants: ResultSet.CONCUR_READ_ONLY or
     * ResultSet.CONCUR_UPDATABLE
     * @param resultSetHoldability one of the following ResultSet
     * constants: ResultSet.HOLD_CURSORS_OVER_COMMIT or
     * ResultSet.CLOSE_CURSORS_AT_COMMIT
     * @return a new PreparedStatement object, containing the
     * pre-compiled SQL statement, that will generate ResultSet objects
     * with the given type, concurrency, and holdability
     * @throws SQLException if a database access error occurs or the
     * given parameters are not ResultSet constants indicating type,
     * concurrency, and holdability
     */
    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        checkNotClosed();
        try {
            MonetPreparedStatement s = new MonetPreparedStatement(this, sql);
            statements.add(s);
            return s;
        } catch (IllegalArgumentException e) {
            throw new SQLException(e.toString(), "M0M03");
        }
    }

    /**
     * Auto-generated keys are not yet currently supported.
     * Throws SQLFeatureNotSupportedException.
     */
    //TODO Auto-generated keys
    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLFeatureNotSupportedException("prepareStatement(String sql, int autoGeneratedKeys)");
    }

    /**
     * Auto-generated keys are not yet currently supported.
     * Throws SQLFeatureNotSupportedException.
     */
    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException("prepareStatement(String sql, int[] columnIndexes)");
    }

    /**
     * Auto-generated keys are not yet currently supported.
     * Throws SQLFeatureNotSupportedException.
     */
    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException("prepareStatement(String sql, String[] columnNames)");
    }

    //Savepoints
    /**
     * Savepoints are not yet currently supported.
     * Throws SQLFeatureNotSupportedException.
     */
    //TODO Savepoints
    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new SQLFeatureNotSupportedException("setSavepoint()");
    }

    /**
     * Savepoints are not yet currently supported.
     * Throws SQLFeatureNotSupportedException.
     */
    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException("setSavepoint(String name)");
    }

    /**
     * Savepoints are not yet currently supported.
     * Throws SQLFeatureNotSupportedException.
     */
    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException("rollback(Savepoint savepoint)");
    }

    /**
     * Savepoints are not yet currently supported.
     * Throws SQLFeatureNotSupportedException.
     */
    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException("releaseSavepoint(Savepoint savepoint)");
    }

    //Create Complex Types
    /**
     * Constructs an object that implements the Clob interface. The
     * object returned initially contains no data.
     *
     * @return a MonetClob instance
     * @throws SQLException - if an object that implements the Clob interface can not be constructed,
     *         this method is called on a closed connection or a database access error occurs.
     */
    @Override
    public Clob createClob() throws SQLException {
        checkNotClosed();
        return new MonetClob("");
    }

    /**
     * Constructs an object that implements the Blob interface. The
     * object returned initially contains no data.
     *
     * @return a MonetBlob instance
     * @throws SQLException - if an object that implements the Blob interface can not be constructed,
     *         this method is called on a closed connection or a database access error occurs.
     */
    @Override
    public Blob createBlob() throws SQLException {
        checkNotClosed();
        return new MonetBlob(new byte[1]);
    }

    /**
     * Arrays are not yet currently supported.
     * Throws SQLFeatureNotSupportedException.
     */
    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw new SQLFeatureNotSupportedException("createArrayOf");
    }

    /**
     * Structs are not yet currently supported.
     * Throws SQLFeatureNotSupportedException.
     */
    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new SQLFeatureNotSupportedException("createStruct");
    }

    /**
     * NClobs are not yet currently supported.
     * Throws SQLFeatureNotSupportedException.
     */
    @Override
    public NClob createNClob() throws SQLException {
        throw new SQLFeatureNotSupportedException("createNClob");
    }

    /**
     * SQL XML is not yet currently supported.
     * Throws SQLFeatureNotSupportedException.
     */
    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw new SQLFeatureNotSupportedException("createSQLXML");
    }
}
