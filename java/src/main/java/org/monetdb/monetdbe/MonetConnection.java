package org.monetdb.monetdbe;

import java.nio.ByteBuffer;
import java.sql.*;
import java.util.*;
import java.util.concurrent.Executor;

public class MonetConnection extends MonetWrapper implements Connection {
    protected ByteBuffer dbNative;

    private int sessiontimeout;
    private int querytimeout;
    private int memorylimit;
    private int nr_threads;
    private boolean autoCommit;

    private String jdbcURL;
    private String username;

    private MonetDatabaseMetaData metaData;
    private SQLWarning warnings;
    private Map<String, Class<?>> typeMap = new HashMap<String, Class<?>>();
    private Properties properties;
    private List<MonetStatement> statements;

    MonetConnection(Properties props) throws SQLException, IllegalArgumentException {
        this.sessiontimeout = Integer.parseInt(props.getProperty("sessiontimeout", "0"));
        this.querytimeout = Integer.parseInt(props.getProperty("querytimeout", "0"));
        this.memorylimit = Integer.parseInt(props.getProperty("memorylimit", "0"));
        this.nr_threads = Integer.parseInt(props.getProperty("nr_threads", "0"));
        this.autoCommit = Boolean.parseBoolean(props.getProperty("autocommit", "true"));
        this.jdbcURL = props.getProperty("jdbc-url");
        this.username = props.getProperty("user", "");

        String error_msg;
        if (props.containsKey("url")) {
            //Remote proxy databases
            String url = props.getProperty("url");
            String host = props.getProperty("host", "localhost");
            int port = Integer.parseInt(props.getProperty("port", "50000"));
            String user = props.getProperty("user", "monetdb");
            String password = props.getProperty("password", "monetdb");
            error_msg = MonetNative.monetdbe_open(url, this, sessiontimeout, querytimeout, memorylimit, nr_threads, host, port, user, password);
        } else {
            //Local directory and in-memory databases
            String path = props.getProperty("path", null);
            error_msg = MonetNative.monetdbe_open(path, this, sessiontimeout, querytimeout, memorylimit, nr_threads);
        }

        if (dbNative == null || error_msg != null) {
            throw new SQLException(error_msg);
        }

        //TODO Should I not set it if the autoCommit variable is the default value?
        //MonetNative.monetdbe_set_autocommit(dbNative, autoCommit ? 1 : 0);
        this.metaData = new MonetDatabaseMetaData(this);
        this.properties = props;
        this.statements = new ArrayList<>();
    }

    protected ByteBuffer getDbNative() {
        return dbNative;
    }

    private void addWarning(final String reason, final String sqlstate) {
        final SQLWarning warn = new SQLWarning(reason, sqlstate);
        if (warnings == null) {
            warnings = warn;
        } else {
            warnings.setNextWarning(warn);
        }
    }

    private void checkNotClosed() throws SQLException {
        if (isClosed())
            throw new SQLException("Connection is closed", "M1M20");
    }

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
    @Override
    public void commit() throws SQLException {
        checkNotClosed();
        if (getAutoCommit())
            throw new SQLException("COMMIT: not allowed in auto commit mode");
        executeCommand("COMMIT");
    }

    @Override
    public void rollback() throws SQLException {
        checkNotClosed();
        if (getAutoCommit())
            throw new SQLException("Operation not permitted in autocommit");
        executeCommand("ROLLBACK");
    }

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

    @Override
    public boolean isClosed() throws SQLException {
        return dbNative == null;
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        checkNotClosed();
        if (executor == null)
            throw new SQLException("executor is null", "M1M05");
        close();
    }

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
            //TODO Do something? We can't throw the exception, and isValid should be false
        } finally {
            if (rs != null)
                rs.close();
            if (st != null)
                st.close();
        }

        return isValid;
    }

    //Metadata sets and gets
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

    @Override
    public boolean getAutoCommit() throws SQLException {
        checkNotClosed();
        //Calling the server instead of returning the Java variable because the value may have changed
        return MonetNative.monetdbe_get_autocommit(dbNative);
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        checkNotClosed();
        return metaData;
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        checkNotClosed();
        if (readOnly) {
            addWarning("cannot setReadOnly(true): read-only Connection mode not supported", "01M08");
        }
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        checkNotClosed();
        return false;
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        //ignore this request as MonetDB does not support catalogs
        throw new SQLFeatureNotSupportedException("setCatalog");
    }

    @Override
    public String getCatalog() throws SQLException {
        // MonetDB does NOT support catalogs
        throw new SQLFeatureNotSupportedException("getCatalog");
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        checkNotClosed();
        if (level != TRANSACTION_SERIALIZABLE) {
            addWarning("MonetDB only supports fully serializable " +
                    "transactions, continuing with transaction level " +
                    "raised to TRANSACTION_SERIALIZABLE", "01M09");
        }
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        checkNotClosed();
        return TRANSACTION_SERIALIZABLE;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        checkNotClosed();
        return warnings;
    }

    @Override
    public void clearWarnings() throws SQLException {
        checkNotClosed();
        warnings = null;
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        checkNotClosed();
        return typeMap;
    }

    //TODO Implement UDTs?
    //A user may enter a custom mapping for a UDT in this type map.
    //When a UDT is retrieved from a data source with the method ResultSet.getObject, the getObject method will check the connection's type map to see if there is an entry for that UDT.
    //If so, the getObject method will map the UDT to the class indicated. If there is no entry, the UDT will be mapped using the standard mapping.
    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        checkNotClosed();
        typeMap = map;
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        checkNotClosed();
        if (holdability != ResultSet.HOLD_CURSORS_OVER_COMMIT)
            throw new SQLFeatureNotSupportedException("setHoldability(CLOSE_CURSORS_AT_COMMIT)");
    }

    @Override
    public int getHoldability() throws SQLException {
        checkNotClosed();
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

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

    @Override
    public String getClientInfo(String name) throws SQLException {
        checkNotClosed();
        return properties.getProperty(name);
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        checkNotClosed();
        return properties;
    }

    //TODO Verify this
    @Override
    public void setSchema(String schema) throws SQLException {
        checkNotClosed();
        if (schema == null || schema.isEmpty())
            throw new SQLException("Missing schema name", "M1M05");
        executeCommand("SET SCHEMA \"" + schema + "\"");
    }

    //TODO Verify this
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

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        checkNotClosed();
        //Using session timeout for now
        this.sessiontimeout = milliseconds;
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        checkNotClosed();
        //Returning session timeout for now
        return this.sessiontimeout;
    }

    @Override
    public String nativeSQL(final String sql) {
        /* there is currently no way to get the native MonetDB rewritten SQL string back, so just return the original string */
        return sql;
    }

    public String getJdbcURL() {
        return jdbcURL;
    }

    //TODO: Get user from the database
    public String getUserName() {
        return username;
    }

    //Statements
    @Override
    public Statement createStatement() throws SQLException {
        return createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return createStatement(resultSetType, resultSetConcurrency, ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        checkNotClosed();
        try {
            MonetStatement s = new MonetStatement(this, resultSetType, resultSetConcurrency, resultSetHoldability);
            statements.add(s);
            return s;
        } catch (IllegalArgumentException e) {
            throw new SQLException(e.toString(), "M0M03");
        }
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        return prepareCall(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return prepareCall(sql, resultSetType, resultSetConcurrency, ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

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

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return prepareStatement(sql, resultSetType, resultSetConcurrency, ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

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

    //TODO Auto-generated keys
    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        checkNotClosed();
        return prepareStatement(sql);
    }

    //TODO Auto-generated keys
    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException("prepareStatement(String sql, int[] columnIndexes)");
    }

    //TODO Auto-generated keys
    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException("prepareStatement(String sql, String[] columnNames)");
    }

    //Savepoints
    //TODO Savepoints
    @Override
    public Savepoint setSavepoint() throws SQLException {
        checkNotClosed();
        return null;
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        checkNotClosed();
        return null;
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        checkNotClosed();
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        checkNotClosed();
    }

    //Create Complex Types
    @Override
    public Clob createClob() throws SQLException {
        checkNotClosed();
        return new MonetClob("");
    }

    @Override
    public Blob createBlob() throws SQLException {
        checkNotClosed();
        return new MonetBlob(new byte[1]);
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw new SQLFeatureNotSupportedException("createArrayOf");
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new SQLFeatureNotSupportedException("createStruct");
    }

    @Override
    public NClob createNClob() throws SQLException {
        throw new SQLFeatureNotSupportedException("createNClob");
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw new SQLFeatureNotSupportedException("createSQLXML");
    }
}
