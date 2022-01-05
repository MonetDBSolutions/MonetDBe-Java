import org.monetdb.monetdbe.MonetBlob;
import org.monetdb.monetdbe.MonetConnection;
import org.monetdb.monetdbe.MonetResultSet;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.util.Calendar;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

class MultiThreadRunTest implements Runnable {
    String tableName;
    int threadNum;
    Connection c;

    public MultiThreadRunTest(Connection c, int threadNum) {
        this.threadNum = threadNum;
        this.tableName = "a_" + threadNum;
        this.c = c;
    }

    @Override
    public void run() {
        try {
            Statement stat = c.createStatement();
            ResultSet rs = null;
            stat.execute("CREATE TABLE " + tableName + " (a INT)");
            System.out.println(1 == stat.executeUpdate("INSERT INTO " + tableName + " VALUES (" + threadNum + ")"));
            rs = stat.executeQuery("SELECT * FROM " + tableName);
            System.out.println(1 == ((MonetResultSet) rs).getRowsNumber());
            while (rs.next())
                System.out.println(threadNum == rs.getInt(1));
            rs = stat.executeQuery("SELECT * FROM  a_0");
            System.out.println(1 == ((MonetResultSet) rs).getRowsNumber());
            while (rs.next())
                System.out.println(0 == rs.getInt(1));
            System.out.println(!rs.next());
            c.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
}

public class RemoteConnectionTests {
    private static final String[] TITLES = {"Iron Man", "The Incredible Hulk", "Iron Man 2", "Thor", "Captain America: The First Avenger", "The Avengers", "Iron Man 3", "Captain America: The Winter Soldier", "Avengers: Age of Ultron", "Captain America: Civil War", "Doctor Strange", "Black Panther", "Avengers: Infinity War"};
    private static final int[] YEARS = {2008, 2008, 2010, 2011, 2011, 2012, 2013, 2014, 2015, 2016, 2016, 2018, 2018};
    private static final String[] FIRSTNAME = {"Robert", "Chris", "Scarlett", "Samuel L.", "Benedict", "Brie", "Chadwick"};
    private static final String[] LASTNAME = {"Downey Jr.", "Evans", "Johansson", "Jackson", "Cumberbatch", "Larson", "Boseman"};
    private static final String[] HERO = {"Iron Man", "Captain America", "Black Widow", "Nick Fury", "Dr. Strange", "Captain Marvel", "Black Panther"};
    private static final int[] AGE = {53, 37, 33, 69, 42, 29, 40};
    private static final int[] ACTORS = {1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 4, 4, 4, 4, 4, 4, 4, 4, 5, 5, 7, 7, 7};
    private static final int[] MOVIES = {1, 2, 3, 6, 7, 9, 10, 13, 5, 6, 8, 9, 10, 13, 3, 6, 8, 9, 10, 13, 1, 3, 4, 5, 6, 8, 9, 13, 11, 13, 10, 12, 13};

    private static void metadata(Connection conn) {
        try {
            DatabaseMetaData dbMeta = conn.getMetaData();

            System.out.println(!dbMeta.getSQLKeywords().isEmpty());
            System.out.println(!dbMeta.getNumericFunctions().isEmpty());
            System.out.println(!dbMeta.getStringFunctions().isEmpty());
            System.out.println(!dbMeta.getSystemFunctions().isEmpty());
            System.out.println(!dbMeta.getTimeDateFunctions().isEmpty());

            try (ResultSet rs = dbMeta.getTableTypes()) {
                System.out.println(((MonetResultSet) rs).getRowsNumber() >= 8);
            }
            try (ResultSet rs = dbMeta.getProcedures(null, null, null)) {
                System.out.println(((MonetResultSet) rs).getRowsNumber() > 70);
                System.out.println(9 == ((MonetResultSet) rs).getColumnsNumber());
            }
            try (ResultSet rs = dbMeta.getProcedureColumns(null, null, null, null)) {
                System.out.println(((MonetResultSet) rs).getRowsNumber() >= 10);
                System.out.println(20 == ((MonetResultSet) rs).getColumnsNumber());
            }
            try (ResultSet rs = dbMeta.getTables(null, null, null, null)) {
                System.out.println(((MonetResultSet) rs).getRowsNumber() >= 10);
                System.out.println(10 == ((MonetResultSet) rs).getColumnsNumber());
            }
            try (ResultSet rs = dbMeta.getSchemas(null, null)) {
                System.out.println(7 == ((MonetResultSet) rs).getRowsNumber());
                System.out.println(2 == ((MonetResultSet) rs).getColumnsNumber());
            }
            try (ResultSet rs = dbMeta.getColumns(null, null, null, null)) {
                System.out.println(((MonetResultSet) rs).getRowsNumber() >= 10);
                System.out.println(24 == ((MonetResultSet) rs).getColumnsNumber());
            }
            try (ResultSet rs = dbMeta.getColumnPrivileges(null, null, null, null)) {
                System.out.println(((MonetResultSet) rs).getRowsNumber() >= 10);
                System.out.println(8 == ((MonetResultSet) rs).getColumnsNumber());
            }
            try (ResultSet rs = dbMeta.getTablePrivileges(null, null, null)) {
                System.out.println(((MonetResultSet) rs).getRowsNumber() >= 10);
                System.out.println(7 == ((MonetResultSet) rs).getColumnsNumber());
            }
            try (ResultSet rs = dbMeta.getPrimaryKeys(null, null, null)) {
                System.out.println(((MonetResultSet) rs).getRowsNumber() >= 9);
                System.out.println(6 == ((MonetResultSet) rs).getColumnsNumber());
            }
            try (ResultSet rs = dbMeta.getImportedKeys(null, null, null)) {
                System.out.println(0 == ((MonetResultSet) rs).getRowsNumber());
                System.out.println(14 == ((MonetResultSet) rs).getColumnsNumber());
            }
            try (ResultSet rs = dbMeta.getTypeInfo()) {
                System.out.println(((MonetResultSet) rs).getRowsNumber() >= 10);
                System.out.println(18 == ((MonetResultSet) rs).getColumnsNumber());
            }
            try (ResultSet rs = dbMeta.getIndexInfo(null, null, null, false, false)) {
                System.out.println(((MonetResultSet) rs).getRowsNumber() >= 10);
                System.out.println(13 == ((MonetResultSet) rs).getColumnsNumber());
            }
            try (ResultSet rs = dbMeta.getFunctions(null, null, null)) {
                System.out.println(((MonetResultSet) rs).getRowsNumber() >= 10);
                System.out.println(6 == ((MonetResultSet) rs).getColumnsNumber());
            }
            try (ResultSet rs = dbMeta.getFunctionColumns(null, null, null, null)) {
                System.out.println(((MonetResultSet) rs).getRowsNumber() >= 10);
                System.out.println(17 == ((MonetResultSet) rs).getColumnsNumber());
            }
            try (ResultSet rs = dbMeta.getClientInfoProperties()) {
                System.out.println(7 == ((MonetResultSet) rs).getRowsNumber());
                System.out.println(4 == ((MonetResultSet) rs).getColumnsNumber());
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void complexInsertAndQueryStatements(String connectionUrl) {
        try (Connection conn = DriverManager.getConnection(connectionUrl, null)) {
            // Create table and insert values
            try (Statement statement = conn.createStatement()) {
                statement.executeUpdate("CREATE TABLE test06 (bd NUMERIC, s STRING, b BLOB, d DATE, t TIME, ts TIMESTAMP);");
                statement.executeUpdate("INSERT INTO test06 VALUES "
                        + "(34589.54, 'hello', '12ff803F', current_date, current_time, current_timestamp), "
                        + "(34012933.888, 'world', '0000803F', str_to_date('23-09-1987', '%d-%m-%Y'), str_to_time('11:40:30', '%H:%M:%S'), str_to_timestamp('23-09-1987 11:40', '%d-%m-%Y %H:%M')), "
                        + "(666.666, 'bye', 'ffffffff', str_to_date('23-09-1990', '%d-%m-%Y'), str_to_time('11:40:35', '%H:%M:%S'), str_to_timestamp('23-09-1990 11:40', '%d-%m-%Y %H:%M')), "
                        + "(NULL, NULL, NULL, NULL, NULL, NULL);");
                long time = Calendar.getInstance().getTimeInMillis();

                // Query table
                try (ResultSet rs = statement.executeQuery("SELECT * FROM test06;")) {
                    rs.next();
                    rs.next();
                    rs.next();
                    rs.next();
                }
                // Clean up
                int result = statement.executeUpdate("DROP TABLE test06;");
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void batchesAndJoinsMovies(Connection conn) {
        try {
            try (Statement statement = conn.createStatement()) {
                statement.addBatch("CREATE TABLE Movies (id SERIAL, title STRING NOT NULL, \"year\" INTEGER NOT NULL);");
                statement.addBatch("CREATE TABLE Actors (id SERIAL, first_name TEXT NOT NULL, last_name TEXT NOT NULL, \"character\" TEXT NOT NULL, age REAL NOT NULL);");
                statement.addBatch("CREATE TABLE MovieActors (id SERIAL, movie_id INTEGER NOT NULL, actor_id INTEGER NOT NULL);");
                long[] updateCounts = statement.executeLargeBatch();
                long[] expectedCounts = {0, 0, 0};
                System.out.println(3 == updateCounts.length);
                System.out.println(updateCounts[0] == 0 && 0 == updateCounts[1] && 0 == updateCounts[2]);
            }

            // Using a Prepared Statement, we can reuse a query for multiple parameters
            // Using addBatch, we can store multiple input parameters, which can then be executed at once
            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO Movies (title, \"year\") VALUES (?, ?);")) {
                for (int i = 0; i < TITLES.length; i++) {
                    ps.setString(1, TITLES[i]);
                    ps.setInt(2, YEARS[i]);
                    ps.addBatch();
                }
                ps.executeBatch();
            }
            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO Actors (first_name, last_name, \"character\", age) VALUES (?, ?, ?, ?);")) {
                for (int i = 0; i < AGE.length; i++) {
                    ps.setString(1, FIRSTNAME[i]);
                    ps.setString(2, LASTNAME[i]);
                    ps.setString(3, HERO[i]);
                    ps.setInt(4, AGE[i]);
                    ps.addBatch();
                }
                ps.executeBatch();
            }
            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO MovieActors (movie_id, actor_id) VALUES (?, ?);")) {
                for (int i = 0; i < ACTORS.length; i++) {
                    ps.setInt(1, MOVIES[i]);
                    ps.setInt(2, ACTORS[i]);
                    ps.addBatch();
                }
                ps.executeBatch();
            }

            // Check some of the data
            try (Statement statement = conn.createStatement();
                 ResultSet rs = statement.executeQuery("SELECT * FROM Movies;")) {
                System.out.println(TITLES.length == ((MonetResultSet) rs).getRowsNumber());
                System.out.println(3 == ((MonetResultSet) rs).getColumnsNumber());
            }
            try (Statement statement = conn.createStatement();
                 ResultSet rs = statement.executeQuery("SELECT * FROM Movies ORDER BY \"year\" DESC;")) {
                System.out.println(TITLES.length == ((MonetResultSet) rs).getRowsNumber());
                System.out.println(3 == ((MonetResultSet) rs).getColumnsNumber());
            }

            try (Statement statement = conn.createStatement();
                 ResultSet rs = statement.executeQuery("SELECT Movies.title, Movies.\"year\", Actors.first_name, Actors.\"character\" FROM MovieActors JOIN Movies ON MovieActors.movie_id = Movies.id JOIN Actors ON MovieActors.actor_id = Actors.id;")) {
                System.out.println(33 == ((MonetResultSet) rs).getRowsNumber());
                System.out.println(4 == ((MonetResultSet) rs).getColumnsNumber());
            }

            // Clean up
            try (Statement statement = conn.createStatement()) {
                statement.executeUpdate("DROP TABLE MovieActors;");
                statement.executeUpdate("DROP TABLE Actors;");
                statement.executeUpdate("DROP TABLE Movies;");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void schema(Connection conn) {
        try {
            System.out.println("sys".equals(conn.getSchema()));
            System.out.println("monetdb".equals(((MonetConnection) conn).getUserName()));

            // Create table and insert values
            try (Statement statement = conn.createStatement()) {
                statement.execute("CREATE SCHEMA test13_voc;");

                conn.setSchema("test13_voc");
                System.out.println("test13_voc".equals(conn.getSchema()));
                System.out.println("monetdb".equals(((MonetConnection) conn).getUserName()));

                conn.setSchema("sys");
                System.out.println("sys".equals(conn.getSchema()));
                System.out.println("monetdb".equals(((MonetConnection) conn).getUserName()));
            }

            try (Statement statement = conn.createStatement()) {
                // Clean up
                statement.execute("DROP SCHEMA test13_voc;");

                System.out.println((0 == statement.getUpdateCount())); // 0: drop succeeded
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void transactions(Connection conn) {
        try {
            try (Statement statement = conn.createStatement()) {
                System.out.println(conn.getAutoCommit());
                statement.executeUpdate("CREATE TABLE test15 (i int);");

                // Begin transaction
                conn.setAutoCommit(false);
                System.out.println(!conn.getAutoCommit());

                statement.executeUpdate("INSERT INTO test15 VALUES (12345);");
                conn.rollback();

                try (ResultSet rs = statement.executeQuery("SELECT * FROM test15;")) {
                    System.out.println(!rs.next());
                }

                statement.executeUpdate("INSERT INTO test15 VALUES (23456);");
                conn.commit();

                try (ResultSet rs = statement.executeQuery("SELECT * FROM test15;")) {
                    System.out.println(rs.next());
                    System.out.println(23456 == rs.getInt(1));
                }

                // Finish transaction
                conn.setAutoCommit(true);
                System.out.println(conn.getAutoCommit());

                // Clean up
                int result = statement.executeUpdate("DROP TABLE test15;");
                System.out.println(1 == result); // 1: because we've dropped a table with 1 record

                System.out.println(-1 == statement.getUpdateCount());
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void multithreadedConnection(Connection conn) {
        try {
            int n = 5;
            ExecutorService executor = Executors.newFixedThreadPool(n);
            for (int i = 0; i < n; i++) {
                Runnable t = null;
                t = new MultiThreadRunTest(conn, i);
                executor.execute(t);
            }
            executor.shutdown();
            try {
                executor.awaitTermination(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            Statement dropStat = conn.createStatement();
            for (int i = 0; i < n; i++) {
                //Cleanup
                int result = dropStat.executeUpdate("DROP TABLE a_" + i + ";");
                System.out.println(result == 0);
            }
            try {
                conn.close();
                System.out.println(conn.isClosed());
            } catch (SQLException e) {
                e.printStackTrace();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void parameterMetadata(Connection conn) {
        try {
            Statement s = conn.createStatement();
            s.execute("CREATE TABLE test19 (i int, l bigint, f real, d double, bd NUMERIC(36,18), s STRING, b BLOB, da DATE);");

            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO test19 VALUES (?,?,?,?,?,?,?,?)")) {
                ParameterMetaData meta = ps.getParameterMetaData();
                System.out.println(8 == meta.getParameterCount());

                if (meta.getPrecision(1) != -1) {
                    //Check Precision and Scale
                    System.out.println(32 == meta.getPrecision(1));
                    System.out.println(0 == meta.getScale(1));
                    System.out.println(64 == meta.getPrecision(2));
                    System.out.println(0 == meta.getScale(2));

                    //TODO Why 24 and 53 instead of 32 and 64?
                    System.out.println(24 == meta.getPrecision(3));
                    System.out.println(0 == meta.getScale(3));
                    System.out.println(53 == meta.getPrecision(4));
                    System.out.println(0 == meta.getScale(4));

                    System.out.println(36 == meta.getPrecision(5));
                    System.out.println(18 == meta.getScale(5));
                    System.out.println(0 == meta.getPrecision(6));
                    System.out.println(0 == meta.getScale(6));
                    System.out.println(0 == meta.getPrecision(7));
                    System.out.println(0 == meta.getScale(7));
                    System.out.println(0 == meta.getPrecision(8));
                    System.out.println(0 == meta.getScale(8));
                }
                //TODO Find better strategy to do different tests for older versions
                //In Jul2021, there is no precision info on Parameter Metadata
                else {
                    for (int i = 0; i < meta.getParameterCount(); i++) {
                        System.out.println(-1 == meta.getPrecision(i + 1));
                        System.out.println(-1 == meta.getScale(i + 1));
                    }

                }

                //Check types (sql, monetdbe, java)
                System.out.println(Types.INTEGER == meta.getParameterType(1));
                System.out.println("monetdbe_int32_t".equals(meta.getParameterTypeName(1)));
                System.out.println(Integer.class.getName().equals(meta.getParameterClassName(1)));
                System.out.println(Types.BIGINT == meta.getParameterType(2));
                System.out.println("monetdbe_int64_t".equals(meta.getParameterTypeName(2)));
                System.out.println(Long.class.getName().equals(meta.getParameterClassName(2)));
                System.out.println(Types.REAL == meta.getParameterType(3));
                System.out.println("monetdbe_float".equals(meta.getParameterTypeName(3)));
                System.out.println(Float.class.getName().equals(meta.getParameterClassName(3)));
                System.out.println(Types.DOUBLE == meta.getParameterType(4));
                System.out.println("monetdbe_double".equals(meta.getParameterTypeName(4)));
                System.out.println(Double.class.getName().equals(meta.getParameterClassName(4)));
                System.out.println(Types.NUMERIC == meta.getParameterType(5));
                //TODO MonetDBe type should return monetdbe_int64 (NUMERIC is not necessarily a int128)
                System.out.println("monetdbe_int128_t".equals(meta.getParameterTypeName(5)));
                System.out.println(BigDecimal.class.getName().equals(meta.getParameterClassName(5)));
                System.out.println(Types.VARCHAR == meta.getParameterType(6));
                System.out.println("monetdbe_str".equals(meta.getParameterTypeName(6)));
                System.out.println(String.class.getName().equals(meta.getParameterClassName(6)));
                System.out.println(Types.BLOB == meta.getParameterType(7));
                System.out.println("monetdbe_blob".equals(meta.getParameterTypeName(7)));
                System.out.println(Blob.class.getName().equals(meta.getParameterClassName(7)));
                System.out.println(Types.DATE == meta.getParameterType(8));
                System.out.println("monetdbe_date".equals(meta.getParameterTypeName(8)));
                System.out.println(Date.class.getName().equals(meta.getParameterClassName(8)));

            }
            s.execute("DROP TABLE test19");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void resultPreparedMetadata(Connection conn) {
        try {
            Statement s = conn.createStatement();
            s.execute("CREATE TABLE test20 (i int, l bigint, f real, d double, bd NUMERIC(36,18), s STRING, b BLOB, da DATE);");
            s.execute("INSERT INTO test20 VALUES (20,60000,20.4321,20934.43029,4398574389.5983798,'string','12ff803F',current_date)");

            try (PreparedStatement ps = conn.prepareStatement("SELECT * FROM test20 WHERE i = ?")) {
                ResultSetMetaData meta = ps.getMetaData();
                System.out.println(8 == meta.getColumnCount());

                if (meta.getPrecision(1) != -1) {
                    //Check Precision and Scale
                    System.out.println(32 == meta.getPrecision(1));
                    System.out.println(0 == meta.getScale(1));
                    System.out.println(64 == meta.getPrecision(2));
                    System.out.println(0 == meta.getScale(2));

                    //TODO Why 24 and 53 instead of 32 and 64?
                    System.out.println(24 == meta.getPrecision(3));
                    System.out.println(0 == meta.getScale(3));
                    System.out.println(53 == meta.getPrecision(4));
                    System.out.println(0 == meta.getScale(4));

                    System.out.println(36 == meta.getPrecision(5));
                    System.out.println(18 == meta.getScale(5));
                    System.out.println(0 == meta.getPrecision(6));
                    System.out.println(0 == meta.getScale(6));
                    System.out.println(0 == meta.getPrecision(7));
                    System.out.println(0 == meta.getScale(7));
                    System.out.println(0 == meta.getPrecision(8));
                    System.out.println(0 == meta.getScale(8));
                }
                //TODO Find better strategy to do different tests for older versions
                //In Jul2021, there is no precision info on Parameter Metadata
                else {
                    for (int i = 0; i < meta.getColumnCount(); i++) {
                        System.out.println(-1 == meta.getPrecision(i + 1));
                        System.out.println(-1 == meta.getScale(i + 1));
                    }

                }

                //Check types (sql, monetdbe, java)
                System.out.println(Types.INTEGER == meta.getColumnType(1));
                System.out.println("monetdbe_int32_t".equals(meta.getColumnTypeName(1)));
                System.out.println(Integer.class.getName().equals(meta.getColumnClassName(1)));
                System.out.println(Types.BIGINT == meta.getColumnType(2));
                System.out.println("monetdbe_int64_t".equals(meta.getColumnTypeName(2)));
                System.out.println(Long.class.getName().equals(meta.getColumnClassName(2)));
                System.out.println(Types.REAL == meta.getColumnType(3));
                System.out.println("monetdbe_float".equals(meta.getColumnTypeName(3)));
                System.out.println(Float.class.getName().equals(meta.getColumnClassName(3)));
                System.out.println(Types.DOUBLE == meta.getColumnType(4));
                System.out.println("monetdbe_double".equals(meta.getColumnTypeName(4)));
                System.out.println(Double.class.getName().equals(meta.getColumnClassName(4)));
                System.out.println(Types.NUMERIC == meta.getColumnType(5));
                //TODO MonetDBe type should return monetdbe_int64 (NUMERIC is not necessarily a int128)
                System.out.println("monetdbe_int128_t".equals(meta.getColumnTypeName(5)));
                System.out.println(BigDecimal.class.getName().equals(meta.getColumnClassName(5)));
                System.out.println(Types.VARCHAR == meta.getColumnType(6));
                System.out.println("monetdbe_str".equals(meta.getColumnTypeName(6)));
                System.out.println(String.class.getName().equals(meta.getColumnClassName(6)));
                System.out.println(Types.BLOB == meta.getColumnType(7));
                System.out.println("monetdbe_blob".equals(meta.getColumnTypeName(7)));
                System.out.println(Blob.class.getName().equals(meta.getColumnClassName(7)));
                System.out.println(Types.DATE == meta.getColumnType(8));
                System.out.println("monetdbe_date".equals(meta.getColumnTypeName(8)));
                System.out.println(Date.class.getName().equals(meta.getColumnClassName(8)));

                System.out.println("i".equals(meta.getColumnName(1)));
                System.out.println("l".equals(meta.getColumnName(2)));
                System.out.println("f".equals(meta.getColumnName(3)));
                System.out.println("d".equals(meta.getColumnName(4)));
                System.out.println("bd".equals(meta.getColumnName(5)));
                System.out.println("s".equals(meta.getColumnName(6)));
                System.out.println("b".equals(meta.getColumnName(7)));
                System.out.println("da".equals(meta.getColumnName(8)));
            }
            s.execute("DROP TABLE test20");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void getObject(Connection conn) {
        try {
            Statement s = conn.createStatement();
            s.executeUpdate("CREATE TABLE test22 (b BOOLEAN, ti TINYINT, si SMALLINT, i INTEGER, l BIGINT, r REAL, f FLOAT,de DECIMAL(32,20), h HUGEINT, s STRING, bl BLOB, d DATE, t TIME, ts TIMESTAMP);");
            PreparedStatement ps = conn.prepareStatement("INSERT INTO test22 VALUES (?,?,?,?,?,?,?,1237831.123879879,9223372036854776800,?,?,?,?,?);");
            long instant = System.currentTimeMillis();
            Date d = new Date(instant);
            Time t = new Time(instant);
            Timestamp ts = new Timestamp(instant);
            BigInteger bigint = BigInteger.valueOf(9223372036854775800L);
            bigint = bigint.add(BigInteger.valueOf(1000));
            BigDecimal bigdec = BigDecimal.valueOf(1237831.123879879);
            long lng = 3000000L;
            float fl = 3287.3289f;
            double db = 328732.328129;

            ps.setObject(1,false);
            ps.setObject(2,1);
            ps.setObject(3,20);
            ps.setObject(4,50000);
            ps.setObject(5,lng);
            ps.setObject(6,fl);
            ps.setObject(7,db);
            //TODO Set BigDec and BigInt are not yet supported (Jan2022)
            //ps.setObject(8,bigdec);
            //ps.setObject(9,bigint);
            ps.setObject(8,"string");
            ps.setObject(9,new MonetBlob("12ff803F"));
            ps.setObject(10,d);
            ps.setObject(11,t);
            ps.setTimestamp(12,ts);

            ResultSet rs = s.executeQuery("SELECT * FROM test22;");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        //String url = "mapi:monetdb://localhost:50000/test";
        String url = "jdbc:monetdb:memory:";
        try (Connection c = DriverManager.getConnection(url)) {
            getObject(c);
            //Passed
            metadata(c);
            schema(c);
            multithreadedConnection(c);
            parameterMetadata(c);
            resultPreparedMetadata(c);

            //Not passed
            //Test06 -> PreparedStatement error (monetdbe thinks there is an extra timestamp parameter to bind)
            //batchesAndJoinsMovies(c);
            //Test15 -> Rollback in undoing a previously committed command (executed with autocommit on)
            //transactions(c);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
