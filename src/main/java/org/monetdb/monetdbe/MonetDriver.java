package org.monetdb.monetdbe;

import java.net.URI;
import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

final public class MonetDriver implements java.sql.Driver {
    //Memory
    //jdbc:monetdb//:memory:
    //Local
    //jdbc:monetdb:<databaseDirectory>
    //TODO Support this syntax
    //jdbc:monetdb://<host>[:<port>]/<databaseDirectory>
    //Remote
    //mapi:monetdb://<host>[:<port>]/<database>
    static final String MONETURL = "jdbc:monetdb:";
    static final String MAPIURL = "mapi:monetdb:";
    static final String MEMORYURL = "jdbc:monetdb://:memory:";

    static {
        try {
            DriverManager.registerDriver(new MonetDriver());
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private Connection connectJDBC(String url, Properties info) throws SQLException {
        if (!url.startsWith(MEMORYURL)) {
            //Local database
            //Remove leading 'jdbc:monetdb:'
            info.put("path",url.substring(13));
        }
        //For in-memory databases, leave the path property NULL
        return new MonetConnection(info);
    }

    private Connection connectMapi(String url, Properties info) throws SQLException {
        final URI uri;
        try {
            //Remove leading "mapi:" and get valid URI
            uri = new URI(url.substring(5));
        } catch (java.net.URISyntaxException e) {
            System.out.println("Uri '" + url + "' not parseable");
            return null;
        }

        //Full URL
        info.put("url",url);

        //Host
        String uri_host = uri.getHost();
        if (uri_host != null)
            info.put("host", uri_host);

        //Port
        int uri_port = uri.getPort();
        if (uri_port > 0)
            info.put("port", Integer.toString(uri_port));

        //TODO Is there any query parameter that should be used (besides user and password)?
        //Check URI query parameters
        final String uri_query = uri.getQuery();
        if (uri_query != null) {
            int pos;
            // handle additional connection properties separated by the & character
            final String args[] = uri_query.split("&");
            for (int i = 0; i < args.length; i++) {
                pos = args[i].indexOf('=');
                if (pos > 0)
                    info.put(args[i].substring(0, pos), args[i].substring(pos + 1));
            }
        }
        return new MonetConnection(info);
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (url == null)
            throw new SQLException("url cannot be null");
        if (!acceptsURL(url))
            return null;
        if (info == null)
            info = new Properties();

        info.setProperty("jdbc-url",url);
        if (url.startsWith(MONETURL)) {
            return connectJDBC(url,info);
        }
        else if (url.startsWith(MAPIURL)) {
            return connectMapi(url,info);
        }
        return null;
    }

    @Override
    public boolean acceptsURL(final String url) {
        return url != null && (url.startsWith(MONETURL) || url.startsWith(MAPIURL));
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(final String url, final Properties info) {
        if (!acceptsURL(url))
            return null;

        final DriverPropertyInfo[] dpi = new DriverPropertyInfo[7];

        DriverPropertyInfo prop = new DriverPropertyInfo("user", info != null ? info.getProperty("user") : null);
        prop.required = false;
        prop.description = "The user loginname to use when authenticating on the database server";
        dpi[0] = prop;

        prop = new DriverPropertyInfo("password", info != null ? info.getProperty("password") : null);
        prop.required = false;
        prop.description = "The password to use when authenticating on the database server";
        dpi[1] = prop;

        prop = new DriverPropertyInfo("session_timeout", "0");
        prop.required = false;
        prop.description = "Graceful terminate the session after a few seconds";
        dpi[2] = prop;

        prop = new DriverPropertyInfo("query_timeout", "0");
        prop.required = false;
        prop.description = "Graceful terminate query after a few seconds";
        dpi[3] = prop;

        prop = new DriverPropertyInfo("memory_limit", "0");
        prop.required = false;
        prop.description = "Top off the amount of RAM to be used, in MB";
        dpi[4] = prop;

        prop = new DriverPropertyInfo("nr_threads", "0");
        prop.required = false;
        prop.description = "Maximum number of worker treads, limits level of parallelism";
        dpi[5] = prop;

        return dpi;
    }

    @Override
    public int getMajorVersion() {
        return 4;
    }

    @Override
    public int getMinorVersion() {
        return 3;
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
