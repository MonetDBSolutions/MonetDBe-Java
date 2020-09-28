package nl.cwi.monetdb.monetdbe;

import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

final public class MonetDriver implements Driver {
    //TODO: Pedro's old code
    // the url kind will be jdbc:monetdb://<host>[:<port>]/<database>
    static final String MONETURL = "jdbc:monetdb://";

    // initialize this class: register it at the DriverManager
    static {
        try {
            DriverManager.registerDriver(new MonetDriver());
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        // url should be of style jdbc:monetdb://<host>/<database>
        if (!acceptsURL(url))
            return null;
        return new MonetConnection(info);
    }

    @Override
    public boolean acceptsURL(final String url) {
        return url != null && url.startsWith(MONETURL);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return 0;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;
    }
}
