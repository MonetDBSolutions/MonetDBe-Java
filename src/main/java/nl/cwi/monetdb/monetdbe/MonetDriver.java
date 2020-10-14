package nl.cwi.monetdb.monetdbe;

import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

final public class MonetDriver implements java.sql.Driver {
    //TODO: Pedro's old code
    // the url kind will be jdbc:monetdb://<host>[:<port>]/<database>
    //static final String MONETURL = "jdbc:monetdb://localhost/";

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

        //TODO De-"hard code" this
        final Properties props = new Properties();
        props.setProperty("dbdir",url);
        props.setProperty("sessiontimeout","0");
        props.setProperty("querytimeout","0");
        props.setProperty("memorylimit","0");
        props.setProperty("nr_threads","0");
        props.setProperty("autocommit","true");

        //TODO
        props.setProperty("uri","");
        props.setProperty("port","");
        props.setProperty("username","");
        props.setProperty("password","");
        props.setProperty("logging","");

        return new MonetConnection(url,props);
    }

    @Override
    public boolean acceptsURL(final String url) {
        return url != null;
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
        return true;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;
    }
}
