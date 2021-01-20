package nl.cwi.monetdb.monetdbe;

import java.net.URI;
import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

final public class MonetDriver implements java.sql.Driver {
    //jdbc:monetdb://<host>[:<port>]/<database>
    //jdbc:monetdb//:memory:
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

        final URI uri;
        try {
            uri = new URI(url.substring(5));
        } catch (java.net.URISyntaxException e) {
            return null;
        }

        if(!uri.toString().equals("monetdb://:memory:")) {
            //TODO Check if everything but the database path is necessary (should be for remote proxy option?)
            info.put("uri", uri.toString());

            final String uri_host = uri.getHost();
            if (uri_host == null)
                return null;
            info.put("host", uri_host);

            int uri_port = uri.getPort();
            if (uri_port > 0)
                info.put("port", Integer.toString(uri_port));

            // check the database
            String uri_path = uri.getPath();
            if (uri_path != null && !uri_path.isEmpty()) {
                uri_path = uri_path.trim();
                System.out.println(uri_path);
                if (!uri_path.isEmpty())
                    info.put("database", uri_path);
            }

            System.out.println(uri.getScheme() + " " + uri.getHost() + " " + uri.getPort() + " " + uri.getPath() + " " + uri.getQuery());
        }

        //TODO Are these used?
        info.setProperty("user","");
        info.setProperty("password","");
        info.setProperty("logging","");

        //Remove leading jdbc:monetdb://

        return new MonetConnection(info);
    }

    @Override
    public boolean acceptsURL(final String url) {
        return url != null && url.startsWith(MONETURL);
    }

    //TODO Change to reflect the options we have available in monetdbe
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

        //TODO Change this
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
