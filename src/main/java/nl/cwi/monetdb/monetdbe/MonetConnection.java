package nl.cwi.monetdb.monetdbe;

import java.net.SocketException;
import java.nio.ByteBuffer;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
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

    MonetConnection(String dbdir, Properties props) throws SQLException, IllegalArgumentException {
        this.dbdir = dbdir;
        this.sessiontimeout = Integer.parseInt((String) props.getOrDefault("sessiontimeout","0"));
        this.querytimeout = Integer.parseInt((String) props.getOrDefault("querytimeout","0"));
        this.memorylimit = Integer.parseInt((String) props.getOrDefault("memorylimit","0"));
        this.nr_threads = Integer.parseInt((String) props.getOrDefault("nr_threads","0"));
        this.autoCommit = Boolean.parseBoolean((String) props.getOrDefault("autocommit","true"));
        //this.dbNative = MonetNative.monetdbe_open(dbdir);
        this.dbNative = MonetNative.monetdbe_open(dbdir,sessiontimeout,querytimeout,memorylimit,nr_threads);
        this.metaData = new MonetDatabaseMetadata();
        this.properties = props;
    }

    public ByteBuffer getDbNative() {
        return dbNative;
    }

    private final void addWarning(final String reason, final String sqlstate) {
        final SQLWarning warng = new SQLWarning(reason, sqlstate);
        if (warnings == null) {
            warnings = warng;
        } else {
            warnings.setNextWarning(warng);
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
        MonetNative.monetdbe_close(dbNative);
        dbNative = null;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return dbNative == null;
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        if (isClosed())
            return;
        if (executor == null)
            throw new SQLException("executor is null", "M1M05");
        close();
    }

    //Pedro's Code
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
        this.autoCommit = autoCommit;
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
        return null;
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
        if(isClosed()) {
            throw new SQLException("Connection is closed.");
        }
        return new MonetStatement(this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return null;
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        return null;
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return null;
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return null;
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return null;
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return null;
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        return null;
    }

    //Savepoints
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
