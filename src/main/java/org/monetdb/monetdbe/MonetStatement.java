package org.monetdb.monetdbe;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

//TODO Check if the statement is closed before doing actions which depend on it
public class MonetStatement extends MonetWrapper implements Statement {
    protected MonetConnection conn;

    protected MonetResultSet resultSet;
    protected SQLWarning warnings;
    protected List<String> batch;

    private int maxRows = 0;
    private long largeMaxRows = 0;
    protected int updateCount = -1;
    protected long largeUpdateCount = -1;
    private int queryTimeout = 0;

    private boolean closed = false;
    private boolean closeOnCompletion = false;

    //TODO Are these actually used?
    private int fetchDirection = ResultSet.FETCH_UNKNOWN;
    private int fetchSize;
    private int resultSetType = ResultSet.TYPE_SCROLL_INSENSITIVE;
    private int resultSetConcurrency = ResultSet.CONCUR_READ_ONLY;
    private int resultSetHoldability = ResultSet.HOLD_CURSORS_OVER_COMMIT;
    private boolean poolable = false;

    public MonetStatement(MonetConnection conn) {
        this.conn = conn;
        this.resultSet = null;
        this.batch = new ArrayList<>();
    }

    public MonetStatement(MonetConnection conn, int resultSetType, int resultSetConcurrency, int resultSetHoldability) {
        this.conn = conn;
        this.resultSet = null;
        this.resultSetType = resultSetType;
        this.resultSetConcurrency = resultSetConcurrency;
        this.resultSetHoldability = resultSetHoldability;
        this.batch = new ArrayList<>();
    }

    public MonetStatement(MonetConnection conn, int resultSetType, int resultSetConcurrency, int resultSetHoldability, int queryTimeout) {
        this.conn = conn;
        this.resultSet = null;
        this.resultSetType = resultSetType;
        this.resultSetConcurrency = resultSetConcurrency;
        this.resultSetHoldability = resultSetHoldability;
        this.batch = new ArrayList<>();
        this.queryTimeout = queryTimeout;
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
    public void checkNotClosed() throws SQLException {
        if (isClosed())
            throw new SQLException("Connection is closed", "M1M20");
    }

    //Close
    @Override
    public void close() throws SQLException {
        //If there is a result set and it is not closed, close it
        if (resultSet != null && !resultSet.isClosed()) {
            resultSet.close();
        }
        closed = true;
    }

    @Override
    public void cancel() throws SQLException {
        throw new SQLFeatureNotSupportedException("Query cancelling is currently not supported by the driver.", "0A000");
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        if (closed)
            throw new SQLException("Cannot call on closed Statement", "M1M20");
        closeOnCompletion = true;
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        if (closed)
            throw new SQLException("Cannot call on closed Statement", "M1M20");
        return closeOnCompletion;
    }

    //Called by the result set object when closed
    //Close open statement if closeOnCompletion() was called
    protected void closeIfComplete () throws SQLException {
        if (!closed && closeOnCompletion) {
            close();
        }
    }

    //Executes
    @Override
    public boolean execute(String sql) throws SQLException {
        this.resultSet = MonetNative.monetdbe_query(conn.getDbNative(),sql,this,false);
        if (this.resultSet!=null) {
            return true;
        }
        //Data manipulation queries
        else if (this.updateCount!=-1){
            return false;
        }
        //Data definition queries
        else {
            return false;
        }
    }

    @Override
    public ResultSet executeQuery(final String sql) throws SQLException {
        if (!execute(sql))
            throw new SQLException("Query did not produce a result set", "M1M19");
        return getResultSet();
    }

    @Override
    public int executeUpdate(final String sql) throws SQLException {
        if (execute(sql))
            throw new SQLException("Query produced a result set", "M1M17");
        return getUpdateCount();
    }

    @Override
    public long executeLargeUpdate(String sql) throws SQLException {
        this.resultSet = MonetNative.monetdbe_query(conn.getDbNative(),sql,this, true);
        if (this.resultSet!=null) {
            throw new SQLException("Query produced a result set", "M1M17");
        }
        else {
            return getLargeUpdateCount();
        }
    }

    //Batch executes
    //TODO Verify this
    @Override
    public int[] executeBatch() throws SQLException {
        if (batch == null || batch.isEmpty()) {
            return new int[0];
        }
        int[] counts = new int[batch.size()];
        int count = -1;

        for (int i = 0; i < batch.size(); i++) {
            String query = batch.get(i);
            try {
                count = executeUpdate(query);
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
    public void addBatch(String sql) throws SQLException {
        if (batch == null) {
            batch = new ArrayList<>();
        }
        batch.add(sql);
    }

    @Override
    public void clearBatch() throws SQLException {
        if (batch != null) {
            batch.clear();
        }
    }

    //TODO Verify this
    @Override
    public long[] executeLargeBatch() throws SQLException {
        if (batch == null || batch.isEmpty()) {
            return new long[0];
        }
        long[] counts = new long[batch.size()];
        long count = -1;

        for (int i = 0; i < batch.size(); i++) {
            String query = batch.get(i);
            try {
                count = executeLargeUpdate(query);
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
    public boolean getMoreResults() throws SQLException {
        return getMoreResults(Statement.CLOSE_CURRENT_RESULT);
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        //TODO GETMORERESULTS
        //Is it possible to have more than one ResultSet returning from a batch query in MonetDBe?
        return false;
    }

    //Auto-generated keys
    //TODO Generated Keys
    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return false;
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return false;
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        return false;
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return 0;
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return 0;
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        return 0;
    }

    @Override
    public long executeLargeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return 0;
    }

    @Override
    public long executeLargeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return 0;
    }

    @Override
    public long executeLargeUpdate(String sql, String[] columnNames) throws SQLException {
        return 0;
    }

    //TODO Generated Keys
    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        return null;
    }

    //Meta gets/sets
    @Override
    public int getUpdateCount() throws SQLException {
        return this.updateCount;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return resultSet;
    }

    //TODO Verify this
    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        if (enable)
            addWarning("setEscapeProcessing: JDBC escape syntax is not supported by this driver", "01M22");
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return queryTimeout;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        //Not possible currently, may be a future feature
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return warnings;
    }

    @Override
    public void clearWarnings() throws SQLException {
        warnings = null;
    }

    //TODO Verify this
    @Override
    public void setCursorName(String name) throws SQLException {
        addWarning("setCursorName: positioned updates/deletes not supported", "01M21");
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        checkNotClosed();
        if (direction == ResultSet.FETCH_FORWARD ||
                direction == ResultSet.FETCH_REVERSE ||
                direction == ResultSet.FETCH_UNKNOWN)
        {
            fetchDirection = direction;
        } else {
            throw new SQLException("Illegal direction: " + direction, "M1M05");
        }
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return fetchDirection;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        if (rows >= 0 && !(getMaxRows() != 0 && rows > getMaxRows())) {
            this.fetchSize = rows;
        } else {
            throw new SQLException("Illegal fetch size value: " + rows, "M1M05");
        }
    }

    @Override
    public int getFetchSize() throws SQLException {
        return fetchSize;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return resultSetConcurrency;
    }

    @Override
    public int getResultSetType() throws SQLException {
        return resultSetType;
    }

    @Override
    public Connection getConnection() throws SQLException {
        return conn;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return resultSetHoldability;
    }

    //TODO Poolable statements
    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        checkNotClosed();
        this.poolable = poolable;
    }

    @Override
    public boolean isPoolable() throws SQLException {
        return this.poolable;
    }

    @Override
    public long getLargeUpdateCount() throws SQLException {
        return largeUpdateCount;
    }

    @Override
    public void setLargeMaxRows(long max) throws SQLException {
        this.largeMaxRows = max;
    }

    @Override
    public long getLargeMaxRows() throws SQLException {
        return largeMaxRows;
    }

    //TODO: Verify this. The old implementation returned a value which overflows the java int
    @Override
    public int getMaxFieldSize() throws SQLException {
        // MonetDB supports null terminated strings of max 2GB, see function: int UTF8_strlen();
        //return 2*1024*1024*1024 - 2;
        return Integer.MAX_VALUE;
    }

    //Pedro's code
    @Override
    public void setMaxFieldSize(final int max) throws SQLException {
        if (max < 0)
            throw new SQLException("Illegal max value: " + max, "M1M05");
        if (max > 0)
            addWarning("setMaxFieldSize: field size limitation not supported", "01M23");
    }

    @Override
    public int getMaxRows() throws SQLException {
        return maxRows;
    }

    //Old code
    @Override
    public void setMaxRows(int max) throws SQLException {
        if (max < 0)
            throw new SQLException("Illegal max value: " + max, "M1M05");
        maxRows = max;
    }
}
