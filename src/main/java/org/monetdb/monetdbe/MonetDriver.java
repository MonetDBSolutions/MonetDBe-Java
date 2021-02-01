package org.monetdb.monetdbe;

import java.net.URI;
import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

final public class MonetDriver implements java.sql.Driver {
    //jdbc:monetdb//:memory:
    //jdbc:monetdb://<host>[:<port>]/<databaseDirectory>
    //TODO These may be wrong, check
    //jdbc:monetdb://<host>[:<port>]/<database>?user=<user>&password=<password>
    //jdbc:mapi:monetdb://<host>[:<port>]/?database=<database>
    static final String MONETURL = "jdbc:monetdb:";
    static final String MAPIURL = "mapi:monetdb:";

    static {
        try {
            DriverManager.registerDriver(new MonetDriver());
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private Connection connectJDBC(String url, Properties info) throws SQLException {
        final URI uri;
        try {
            //Remove leading "jdbc:" and get valid URI
            uri = new URI(url.substring(5));
        } catch (java.net.URISyntaxException e) {
            System.out.println("Uri '" + url.substring(5) + "' not parseable");
            return null;
        }

        //Local Database
        if(!uri.getAuthority().equals(":memory:")) {
            //TODO Make sure we only need the path for local databases
            //Check database path
            String uri_path = uri.getPath();
            if (uri_path != null && !uri_path.isEmpty()) {
                uri_path = uri_path.trim();
                if (!uri_path.isEmpty())
                    info.put("path", uri_path);
            }
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
            System.out.println("Uri '" + url.substring(5) + "' not parseable");
            return null;
        }

        info.put("url","mapi:" + uri.toString());

        final String uri_host = uri.getHost();
        if (uri_host == null)
            return null;
        info.put("host", uri_host);

        int uri_port = uri.getPort();
        if (uri_port > 0)
            info.put("port", Integer.toString(uri_port));

        //Check database path
        String uri_path = uri.getPath();
        if (uri_path != null && !uri_path.isEmpty()) {
            uri_path = uri_path.trim();
            if (!uri_path.isEmpty())
                info.put("path", uri_path);
        }

        //Check URI query
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
        for (String s : info.stringPropertyNames()) {
            System.out.println(s + " " + info.getProperty(s));
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
