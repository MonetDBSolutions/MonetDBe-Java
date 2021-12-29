package org.monetdb.monetdbe;

import java.net.URI;
import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * A JDBC Driver for the embedded version of the MonetDB RDBMS.
 *
 * This driver will be used by the DriverManager to determine if an URL
 * is to be handled by this driver, and if it does, then this driver
 * will supply a Connection suitable for MonetDB-embedded.
 *
 * <p>This Driver supports in-memory databases, local file databases and
 * connection to another MonetDB instance through its MAPI URL.
 * Valid MonetDBe-Java URLs:</p>
 * <ul>
 *   <li>In-memory database: {@code jdbc:monetdb:memory:}</li>
 *   <li>Local file database: {@code jdbc:monetdb:file:/path/to/directory/}</li>
 *   <li>Remote database: {@code mapi:monetdb://&lt;host&gt;[:&lt;port&gt;]/&lt;database&gt;}</li>
 * </ul>
 * where [:&lt;port&gt;] denotes that a port is optional. If not
 * given, the default (50000) will be used.
 *
 * <p>Additional connection properties can be set in the URL query, with the following format:</p>
 * {@code
 * jdbc:monetdb:memory:?property=propertyValue
 * }
 *
 * Or they can be set through the Properties object passed as an argument to DriverManager.getConnection():
 * <pre>{@code
 * Properties props = new Properties();
 * props.setProperty("autocommit","false");
 * Connection c = DriverManager.getConnection("jdbc:monetdb:memory:", props);
 * }</pre>
 *
 * Available properties:
 * <ul>
 *     <li><b>session_timeout</b> - Session timeout, time in milliseconds to terminate session</li>
 *     <li><b>query_timeout</b> - Query timeout, time in milliseconds to terminate single query</li>
 *     <li><b>memory_limit</b> - Memory limit in MBs</li>
 *     <li><b>nr_threads</b> - Maximum number of worker treads</li>
 *     <li><b>autocommit</b> - Autocommit mode</li>
 *     <li><b>log_file</b> - Path to file to log to</li>
 * </ul>
 * Remote connection properties:
 * <ul>
 *     <li><b>user</b> - Username for remote connections</li>
 *     <li><b>password</b> - Password for remote connections</li>
 * </ul>
 */
final public class MonetDriver implements java.sql.Driver {
    //Memory
    //jdbc:monetdb:memory:?property=propertyValue
    //Local
    //jdbc:monetdb:file:<databaseDirectory>?property=propertyValue
    //Remote
    //mapi:monetdb://<host>[:<port>]/<database>?property=propertyValue
    /** The prefix of an in-memory or local file MonetDB URL */
    static final String MONETURL = "jdbc:monetdb:";
    /** The prefix of a MAPI URL (remote connection) */
    static final String MAPIURL = "mapi:monetdb:";
    /** The in-memory database URL */
    static final String MEMORYURL = "jdbc:monetdb:memory:";
    /** The prefix to a file database URL */
    static final String FILEURL = "jdbc:monetdb:file:";

    static {
        try {
            DriverManager.registerDriver(new MonetDriver());
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void parseOptions (String urlQuery, Properties info) {
        //Check URI query parameters (user, password, other options)
        if (urlQuery != null) {
            int pos;
            // handle additional connection properties separated by the & character
            final String args[] = urlQuery.split("&");
            for (int i = 0; i < args.length; i++) {
                pos = args[i].indexOf('=');
                if (pos > 0)
                    info.put(args[i].substring(0, pos).toLowerCase(), args[i].substring(pos + 1));
            }
        }
    }

    private Connection connectJDBC(String url, Properties info) throws SQLException {
        if (url.startsWith(MEMORYURL)) {
            //For in-memory databases, leave the path property NULL
            info.put("connection_type","memory");
        }
        else if (url.startsWith(FILEURL)){
            //Local database
            //Remove leading 'jdbc:monetdb:file:' from directory path
            String path = url.substring(FILEURL.length());
            if (path.indexOf('?') != -1)
                //Remove URL query from end of string if it exists
                info.put("path",path.substring(0,path.indexOf('?')));
            else
                info.put("path",path);
            info.put("connection_type","file");
        }
        else {
            return null;
        }

        //Parse additional options in URL query string
        if (url.contains("?")) {
            parseOptions(url.substring(url.lastIndexOf('?') +1),info);
        }
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

        //Host
        String uri_host = uri.getHost();
        if (uri_host != null)
            info.put("host", uri_host);

        //Port
        int uri_port = uri.getPort();
        if (uri_port > 0)
            info.put("port", Integer.toString(uri_port));

        //Database
        String database = uri.getPath();
        if (database != null)
            //Remove the leading / from the database name
            info.put("database",database.substring(1));

        //Parse additional options in URL query string
        final String uri_query = uri.getQuery();
        parseOptions(uri_query,info);

        info.put("connection_type","remote");
        return new MonetConnection(info);
    }

    /**
     * Attempts to make a database connection to the given URL. The driver
     * should return "null" if it realizes it is the wrong kind of driver to
     * connect to the given URL. This will be common, as when the JDBC driver
     * manager is asked to connect to a given URL it passes the URL to each
     * loaded driver in turn.
     *
     * The driver should throw an SQLException if it is the right driver to
     * connect to the given URL but has trouble connecting to the database.
     *
     * The java.util.Properties argument can be used to pass arbitrary string
     * tag/value pairs as connection arguments. Normally at least "user" and
     * "password" properties should be included in the Properties object.
     *
     * @param url the URL of the database to which to connect
     * @param info a list of arbitrary string tag/value pairs as connection
     *        arguments. Normally at least a "user" and "password" property
     *        should be included
     * @return a Connection object that represents a connection to the URL
     * @throws SQLException if a database access error occurs
     */
    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (url == null)
            throw new SQLException("url cannot be null");
        if (!acceptsURL(url))
            return null;
        if (info == null)
            info = new Properties();

        info.setProperty("jdbc_url",url);
        if (url.startsWith(MONETURL)) {
            return connectJDBC(url,info);
        }
        else if (url.startsWith(MAPIURL)) {
            return connectMapi(url,info);
        }
        return null;
    }

    /**
     * Retrieves whether the driver thinks that it can open a connection to the
     * given URL. Typically drivers will return true if they understand the
     * subprotocol specified in the URL and false if they do not.
     *
     * @param url the URL of the database
     * @return true if this driver understands the given URL; false otherwise
     */
    @Override
    public boolean acceptsURL(final String url) {
        return url != null && (url.startsWith(MONETURL) || url.startsWith(MAPIURL));
    }

    /**
     * Gets information about the possible properties for this driver.
     *
     * The getPropertyInfo method is intended to allow a generic GUI tool to
     * discover what properties it should prompt a human for in order to get
     * enough information to connect to a database. Note that depending on the
     * values the human has supplied so far, additional values may become
     * necessary, so it may be necessary to iterate though several calls to the
     * getPropertyInfo method.
     *
     * @param url the URL of the database to which to connect
     * @param info a proposed list of tag/value pairs that will be sent on
     *        connect open
     * @return an array of DriverPropertyInfo objects describing possible
     *         properties. This array may be an empty array if no properties
     *         are required.
     */
    @Override
    public DriverPropertyInfo[] getPropertyInfo(final String url, final Properties info) {
        if (!acceptsURL(url))
            return null;

        final DriverPropertyInfo[] dpi = new DriverPropertyInfo[11];

        DriverPropertyInfo prop;
        prop = new DriverPropertyInfo("session_timeout", info != null ? info.getProperty("session_timeout") : "0");
        prop.required = false;
        prop.description = "Graceful terminate the session after a few seconds";
        dpi[0] = prop;

        prop = new DriverPropertyInfo("query_timeout",  info != null ? info.getProperty("query_timeout") : "0");
        prop.required = false;
        prop.description = "Graceful terminate query after a few seconds";
        dpi[1] = prop;

        prop = new DriverPropertyInfo("memory_limit",  info != null ? info.getProperty("memory_limit") : "0");
        prop.required = false;
        prop.description = "Top off the amount of RAM to be used, in MB";
        dpi[2] = prop;

        prop = new DriverPropertyInfo("nr_threads",  info != null ? info.getProperty("nr_threads") : "0");
        prop.required = false;
        prop.description = "Maximum number of worker treads, limits level of parallelism";
        dpi[3] = prop;

        prop = new DriverPropertyInfo("autocommit", info != null ? info.getProperty("autocommit") : "true");
        prop.required = false;
        prop.description = "If the autocommit mode is on or off.";
        dpi[4] = prop;

        prop = new DriverPropertyInfo("log_file", info != null ? info.getProperty("log_file") : "/tmp/mdbe.log");
        prop.required = false;
        prop.description = "Path to file to log to.";
        dpi[5] = prop;

        prop = new DriverPropertyInfo("user", info != null ? info.getProperty("user") : null);
        prop.required = false;
        prop.description = "Remote Connection: The user login name to use when authenticating on the database server";
        dpi[6] = prop;

        prop = new DriverPropertyInfo("password", info != null ? info.getProperty("password") : null);
        prop.required = false;
        prop.description = "Remote Connection: The password to use when authenticating on the database server";
        dpi[7] = prop;

        return dpi;
    }

    /**
     * Retrieves the driver's major version number. Initially this should be 1.
     *
     * @return this driver's major version number
     */
    @Override
    public int getMajorVersion() {
        return 1;
    }

    /**
     * Gets the driver's minor version number. Initially this should be 0.
     *
     * @return this driver's minor version number
     */
    @Override
    public int getMinorVersion() {
        return 11;
    }

    static final int getDriverMajorVersion() {
        return 1;
    }

    static final int getDriverMinorVersion() {
        return 11;
    }

    /**
     * Gets the driver's full version number as a String.
     *
     * @return this driver's full version number
     */
    public static final String getDriverVersion() {
        return getDriverMajorVersion() + "." + getDriverMinorVersion();
    }

    static final int getDatabaseMajorVersion() {
        return 11;
    }

    static final int getDatabaseMinorVersion() {
        return 40;
    }

    /**
     * Gets the database's full version number as a String.
     *
     * @return this database's full version number
     */
    public static final String getDatabaseVersion() {
        return getDatabaseMajorVersion() + "." + getDatabaseMinorVersion();
    }

    /**
     * Reports whether this driver is a genuine JDBC Compliant&trade; driver. A
     * driver may only report true here if it passes the JDBC compliance tests;
     * otherwise it is required to return false.
     *
     * @return true if this driver is JDBC Compliant; false otherwise
     */
    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    /**
     * Return the parent Logger of all the Loggers used by this data source (not supported).
     *
     * @return the parent Logger for this data source
     * @throws SQLFeatureNotSupportedException if the data source does
     *         not use java.util.logging
     */
    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("getParentLogger");
    }
}
