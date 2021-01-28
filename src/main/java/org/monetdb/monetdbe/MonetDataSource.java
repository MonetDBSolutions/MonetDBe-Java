package org.monetdb.monetdbe;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

//TODO I think I'm not defining this correctly
public class MonetDataSource extends MonetWrapper implements DataSource {
    private String url;
    private final MonetDriver driver;

    //TODO use loginTimeout?
    private int loginTimeout = 0;

    private String user;
    private String password;

    public MonetDataSource() {
        user = "monetdb";
        password = "monetdb";
        url = "jdbc:monetdb://:memory:";
        driver = new MonetDriver();
    }

    @Override
    public Connection getConnection() throws SQLException {
        return getConnection(user,password);
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        final Properties props = new Properties();
        props.put("user", username);
        props.put("password", password);

        return driver.connect(url, props);
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        this.loginTimeout = seconds;
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return loginTimeout;
    }

    //TODO LogWriter
    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return null;
    }

    //TODO LogWriter
    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {

    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("getParentLogger");
    }
}
