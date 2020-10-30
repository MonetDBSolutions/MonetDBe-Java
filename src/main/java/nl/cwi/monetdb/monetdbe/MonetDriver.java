package nl.cwi.monetdb.monetdbe;

import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

final public class MonetDriver implements java.sql.Driver {
    //jdbc:monetdb://<host>[:<port>]/<database>
    //jdbc:monetdb:memory:
    static final String MONETURL = "jdbc:monetdb://";

    static {
        try {
            DriverManager.registerDriver(new MonetDriver());
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url))
            return null;

        final Properties props = new Properties();
        //Remove leading jdbc:monetdb://
        //TODO: If dbdir is an uri, we should parse it differently
        props.setProperty("dbdir",url.substring(16));
        props.setProperty("sessiontimeout","0");
        props.setProperty("querytimeout","0");
        props.setProperty("memorylimit","0");
        props.setProperty("nr_threads","0");
        props.setProperty("autocommit","true");
        props.setProperty("uri","");
        props.setProperty("port","");
        props.setProperty("username","");
        props.setProperty("password","");
        props.setProperty("logging","");

        if (info != null) {
            info.putAll(props);
        }
        else {
            info = props;
        }

        return new MonetConnection(url,info);
    }

    @Override
    public boolean acceptsURL(final String url) {
        return url != null && url.startsWith(MONETURL);
    }

    //Pedro's code (altered)
    @Override
    public DriverPropertyInfo[] getPropertyInfo(final String url, final Properties info) {
        if (!acceptsURL(url))
            return null;

        final String[] boolean_choices = new String[] { "true", "false" };
        final DriverPropertyInfo[] dpi = new DriverPropertyInfo[7];

        DriverPropertyInfo prop = new DriverPropertyInfo("user", info != null ? info.getProperty("user") : null);
        prop.required = false;
        prop.description = "The user loginname to use when authenticating on the database server";
        dpi[0] = prop;

        prop = new DriverPropertyInfo("password", info != null ? info.getProperty("password") : null);
        prop.required = false;
        prop.description = "The password to use when authenticating on the database server";
        dpi[1] = prop;

        //TODO Verify this
        prop = new DriverPropertyInfo("session_timeout", "0");
        prop.required = false;
        prop.description = "Graceful terminate the session after a few seconds";
        dpi[2] = prop;

        prop = new DriverPropertyInfo("query_timeout", "0");
        prop.required = false;
        prop.description = "Graceful terminate query after a few seconds"; // this corresponds to the Connection.setNetworkTimeout() method introduced in JDBC 4.1
        dpi[3] = prop;

        prop = new DriverPropertyInfo("memory_limit", "0");
        prop.required = false;
        prop.description = "Top off the amount of RAM to be used, in MB";
        dpi[4] = prop;

        prop = new DriverPropertyInfo("nr_threads", "0");
        prop.required = false;
        prop.description = "Maximum number of worker treads, limits level of parallelism";
        dpi[5] = prop;

        prop = new DriverPropertyInfo("language", "sql");
        prop.required = false;
        prop.description = "What language to use for MonetDB conversations (experts only)";
        prop.choices = new String[] { "sql", "mal" };
        dpi[6] = prop;

        return dpi;
    }

    @Override
    public int getMajorVersion() {
        return 1;
    }

    @Override
    public int getMinorVersion() {
        return 1;
    }

    @Override
    public boolean jdbcCompliant() {
        // We're not fully JDBC compliant, but what we support is compliant
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("getParentLogger");
    }
}
