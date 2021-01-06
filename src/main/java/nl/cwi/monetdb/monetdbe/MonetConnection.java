package nl.cwi.monetdb.monetdbe;

import java.net.SocketException;
import java.nio.ByteBuffer;
import java.sql.*;
import java.util.*;
import java.util.concurrent.Executor;

public class MonetConnection extends MonetWrapper implements Connection {
    private ByteBuffer dbNative;
    private String dbdir;
    private int sessiontimeout;
    private int querytimeout;
    private int memorylimit;
    private int nr_threads;
    private boolean autoCommit;
    private MonetDatabaseMetadata metaData;
    private SQLWarning warnings;
    private Map<String,Class<?>> typeMap = new HashMap<String,Class<?>>();
    private Properties properties;
    private List<MonetStatement> statements;

    MonetConnection(String dbdir, Properties props) throws SQLException, IllegalArgumentException {
        this.dbdir = dbdir;
        this.sessiontimeout = Integer.parseInt((String) props.getOrDefault("sessiontimeout","0"));
        this.querytimeout = Integer.parseInt((String) props.getOrDefault("querytimeout","0"));
        this.memorylimit = Integer.parseInt((String) props.getOrDefault("memorylimit","0"));
        this.nr_threads = Integer.parseInt((String) props.getOrDefault("nr_threads","0"));
        this.autoCommit = Boolean.parseBoolean((String) props.getOrDefault("autocommit","true"));
        //this.dbNative = MonetNative.monetdbe_open(dbdir);
        this.dbNative = MonetNative.monetdbe_open(dbdir,sessiontimeout,querytimeout,memorylimit,nr_threads);
        MonetNative.monetdbe_set_autocommit(dbNative,autoCommit ? 1 : 0);
        this.metaData = new MonetDatabaseMetadata();
        this.properties = props;
        this.statements = new ArrayList<>();
    }

    public ByteBuffer getDbNative() {
        return dbNative;
    }

    private final void addWarning(final String reason, final String sqlstate) {
        final SQLWarning warn = new SQLWarning(reason, sqlstate);
        if (warnings == null) {
            warnings = warn;
        } else {
            warnings.setNextWarning(warn);
        }
    }

    //TODO Add this check to functions to verify connection is not closed
    private void checkNotClosed() throws SQLException {
        if (isClosed())
            throw new SQLException("Connection is closed", "M1M20");
    }

    private void executeCommand (String sql) throws SQLException {
        Statement st = null;
        try {
            st = createStatement();
            if (st != null) {
                st.execute(sql);
            }
        } finally {
            st.close();
        }
    }

    //Transactions and closing
    @Override
    public void commit() throws SQLException {
        executeCommand("COMMIT");
    }

    @Override
    public void rollback() throws SQLException {
        executeCommand("ROLLBACK");
    }

    @Override
    public void close() throws SQLException {
        if(isClosed()) {
            throw new SQLException("Connection already closed.");
        }
        for(MonetStatement s : statements) {
            s.close();
        }
        MonetNative.monetdbe_close(dbNative);
        dbNative = null;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return dbNative == null;
    }

    //TODO
    //It is possible that the aborting and releasing of the resources that are held by the connection can take an extended period of time.
    //When the abort method returns, the connection will have been marked as closed and the Executor that was passed as a parameter to abort may still be executing tasks to release resources.
    @Override
    public void abort(Executor executor) throws SQLException {
        if (isClosed())
            return;
        if (executor == null)
            throw new SQLException("executor is null", "M1M05");
        close();
    }

    //Pedro's Code
    //TODO Check this
    @Override
    public boolean isValid(int timeout) throws SQLException {
        if (timeout < 0)
            throw new SQLException("timeout is less than 0", "M1M05");
        if (isClosed())
            return false;

        // ping monetdb server using query: select 1;
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
        } finally {
            rs.close();
            st.close();
        }
        return isValid;
    }

    //Metadata sets and gets
    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        if (autoCommit != this.autoCommit) {
            this.autoCommit = autoCommit;
            MonetNative.monetdbe_set_autocommit(dbNative,autoCommit ? 1 : 0);
        }
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return autoCommit;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return metaData;
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        if (readOnly) {
            addWarning("cannot setReadOnly(true): read-only Connection mode not supported", "01M08");
        }
    }

    @Override
    public boolean isReadOnly() throws SQLException {
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
        if (level != TRANSACTION_SERIALIZABLE) {
            addWarning("MonetDB only supports fully serializable " +
                    "transactions, continuing with transaction level " +
                    "raised to TRANSACTION_SERIALIZABLE", "01M09");
        }
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return TRANSACTION_SERIALIZABLE;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return warnings;
    }

    @Override
    public void clearWarnings() throws SQLException {
        warnings = null;
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return typeMap;
    }

    //TODO Implement UDTs?
    //A user may enter a custom mapping for a UDT in this type map.
    //When a UDT is retrieved from a data source with the method ResultSet.getObject, the getObject method will check the connection's type map to see if there is an entry for that UDT.
    //If so, the getObject method will map the UDT to the class indicated. If there is no entry, the UDT will be mapped using the standard mapping.
    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        typeMap = map;
    }

    //TODO Verify holdability (Pedro's code)
    @Override
    public void setHoldability(int holdability) throws SQLException {
        if (holdability != ResultSet.HOLD_CURSORS_OVER_COMMIT)
            throw new SQLFeatureNotSupportedException("setHoldability(CLOSE_CURSORS_AT_COMMIT)");
    }

    @Override
    public int getHoldability() throws SQLException {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        //TODO Can you change client properties with the current API? We only pass the monetdbe_options struct on opening the database
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        //TODO Can you change client properties with the current API? We only pass the monetdbe_options struct on opening the database
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return properties.getProperty(name);
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return properties;
    }

    //Pedro's code
    @Override
    public void setSchema(String schema) throws SQLException {
        if (schema == null || schema.isEmpty())
            throw new SQLException("Missing schema name", "M1M05");
        executeCommand("SET SCHEMA \"" + schema + "\"");
    }

    //Pedro's code
    @Override
    public String getSchema() throws SQLException {
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
        //TODO Can you change the network timeout (query timeout) with the current API? We only pass the monetdbe_options struct on opening the database
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return querytimeout;
    }

    //TODO Verify this
    //Pedro's code
    @Override
    public String nativeSQL(final String sql) {
        /* there is currently no way to get the native MonetDB rewritten SQL string back, so just return the original string */
        /* in future we may replace/remove the escape sequences { <escape-type> ...} before sending it to the server */
        return sql;
    }

    //Statements
    @Override
    public Statement createStatement() throws SQLException {
        return createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return createStatement(resultSetType,resultSetConcurrency,ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    //TODO Do we need to use these configurations? It seems like they were all hardcoded configs in Pedro's code
    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        checkNotClosed();
        try {
            MonetStatement s = new MonetStatement(this,resultSetType,resultSetConcurrency,resultSetHoldability);
            statements.add(s);
            return s;
        } catch (IllegalArgumentException e) {
            throw new SQLException(e.toString(), "M0M03");
        }
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        return prepareCall(sql,ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return prepareCall(sql,resultSetType,resultSetConcurrency,ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        checkNotClosed();
        try {
            MonetCallableStatement s = new MonetCallableStatement(this,sql);
            statements.add(s);
            return s;
        } catch (IllegalArgumentException e) {
            throw new SQLException(e.toString(), "M0M03");
        }
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return prepareStatement(sql,ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return prepareStatement(sql,resultSetType,resultSetConcurrency,ResultSet.HOLD_CURSORS_OVER_COMMIT);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        checkNotClosed();
        try {
            MonetPreparedStatement s = new MonetPreparedStatement(this,sql);
            statements.add(s);
            return s;
        } catch (IllegalArgumentException e) {
            throw new SQLException(e.toString(), "M0M03");
        }
    }

    //TODO Verify these 3 functions (followed what Pedro did)
    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException("prepareStatement(String sql, int[] columnIndexes)");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException("prepareStatement(String sql, String[] columnNames)");
    }

    //Savepoints
    //TODO Savepoints
    @Override
    public Savepoint setSavepoint() throws SQLException {
        return null;
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        return null;
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {

    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {

    }

    //Types
    //TODO Create types
    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return null;
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return null;
    }

    @Override
    public Clob createClob() throws SQLException {
        return null;
    }

    @Override
    public Blob createBlob() throws SQLException {
        return null;
    }

    @Override
    public NClob createNClob() throws SQLException {
        return null;
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        return null;
    }
}
