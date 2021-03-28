package org.monetdb.monetdbe;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

//TODO Implement correctly?
public class MonetDataSource extends MonetWrapper implements DataSource {
    private String url;
    private final MonetDriver driver;

    private int loginTimeout = 0;

    private String user;
    private String password;
    private String description;

    public MonetDataSource() {
        this.user = "monetdb";
        this.password = "monetdb";
        this.url = "jdbc:monetdb://:memory:";
        this.description = "MonetDB embedded database";
        this.driver = new MonetDriver();
    }

    public MonetDataSource(String url) {
        this.url = url;
        this.user = "monetdb";
        this.password = "monetdb";
        this.description = "MonetDB embedded database";
        this.driver = new MonetDriver();
    }

    public MonetDataSource(String url, String user, String password) {
        this.url = url;
        this.user = user;
        this.password = password;
        this.description = "MonetDB embedded database";
        this.driver = new MonetDriver();
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

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return null;
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {

    }

    public String getUrl() {
        return url;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public String getDescription() {
        return description;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("getParentLogger");
    }
}
