import org.monetdb.monetdbe.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.util.Properties;

public class TestMonetDBeJava {
    private static void populateDBTable(MonetConnection c) {
        try {
            MonetStatement s = (MonetStatement) c.createStatement();

            System.out.println("Create table");
            s.execute("CREATE TABLE a (b boolean, s smallint, i int, l bigint, r real, f float, st string, da date, t time, ts timestamp);");

            System.out.println("Insert into\n");
            s.execute("INSERT INTO a VALUES " +
                    "(true, 2, 3, 5, 1.0, 1.66,'hey1',str_to_date('23-09-1987', '%d-%m-%Y'),str_to_time('11:40:30', '%H:%M:%S'),str_to_timestamp('23-09-1987 11:40', '%d-%m-%Y %H:%M')), " +
                    "(true, 4, 6, 10, 2.5, 3.643,'hey2',str_to_date('23-09-1990', '%d-%m-%Y'),str_to_time('11:40:35', '%H:%M:%S'),str_to_timestamp('23-09-1990 11:40', '%d-%m-%Y %H:%M')), " +
                    "(false, 8, 12, 20, 25.25, 372.325,'hey3',str_to_date('24-09-2020', '%d-%m-%Y'),str_to_time('12:01:59', '%H:%M:%S'),str_to_timestamp('24-09-2007 12:01', '%d-%m-%Y %H:%M')), " +
                    "(false, 16, 24, 40, 255.255, 2434.432,'hey4',current_date,current_time,current_timestamp)," +
                    "(false, null, 1, 1, 1, null,'hey5',current_date,current_time,current_timestamp);");

            System.out.println("Update count: " + s.getUpdateCount());
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void dropDBTable(MonetConnection c) {
        try {
            MonetStatement s = (MonetStatement) c.createStatement();
            s.execute("DROP TABLE a;");
            System.out.println("Drop count: " + s.getUpdateCount());
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void queryDBStatement (MonetConnection c) {
        try {
            MonetStatement s = (MonetStatement) c.createStatement();
            s.executeQuery("SELECT * FROM a;");
            MonetResultSet rs = (MonetResultSet) s.getResultSet();
            System.out.println("Select resultSet: ");
            rs.beforeFirst();
            while (rs.next()) {
                System.out.println("Row " + rs.getRow());
                System.out.println("Bool: " + rs.getBoolean(0));
                System.out.println("Short: " + rs.getShort(1));
                System.out.println("Int: " + rs.getInt(2));
                System.out.println("Long: " + rs.getLong(3));
                System.out.println("Float: " + rs.getFloat(4));
                System.out.println("Double: " + rs.getDouble(5));
                System.out.println("String: " + rs.getString(6));
                System.out.println("Date: " + rs.getDate(7));
                System.out.println("Time: " + rs.getTime(8));
                System.out.println("Timestamp: " + rs.getTimestamp(9));
                System.out.println();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void queryDBPreparedStatement (MonetConnection c) {
        try {
            System.out.println("Preparing statement (Normal query)");
            MonetPreparedStatement ps = (MonetPreparedStatement) c.prepareStatement("SELECT st, i, r FROM a WHERE i < ? AND r < ? AND st <> ? AND s IS NOT NULL");
            ps.setInt(1,20);
            ps.setFloat(2,30.2f);
            ps.setString(3,"hey2");
            ps.executeQuery();
            MonetResultSet rs = (MonetResultSet) ps.getResultSet();

            rs.beforeFirst();
            System.out.println("\nPrepared statement resultSet:");
            while (rs.next()) {
                System.out.println("String: " + rs.getString(0));
                System.out.println("Int: " + rs.getInt(1));
                System.out.println("Float: " + rs.getFloat(2));
                System.out.println();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void insertDBPreparedStatementNulls (MonetConnection c) {
        try {
            System.out.println("Preparing statement (Insert query)");
            MonetPreparedStatement ps = (MonetPreparedStatement) c.prepareStatement("INSERT INTO a VALUES (?, ?, ?, ?, ?, ?,?,?,?,?)");
            ps.setNull(1,Types.BOOLEAN);
            ps.setNull(2,Types.SMALLINT);
            ps.setNull(3,Types.INTEGER);
            ps.setNull(4,Types.BIGINT);
            ps.setNull(5,Types.FLOAT);
            ps.setNull(6,Types.DOUBLE);
            ps.setNull(7,Types.VARCHAR);
            ps.setNull(8,Types.DATE);
            ps.setNull(9,Types.TIME);
            ps.setNull(10,Types.TIMESTAMP);
            ps.executeUpdate();
            int affected_rows = ps.getUpdateCount();
            System.out.println("Prepared statement update count: " + affected_rows + "\n");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void insertDBPreparedStatementDate (MonetConnection c) {
        try {
            System.out.println("Preparing statement (Insert query)");
            MonetPreparedStatement ps = (MonetPreparedStatement) c.prepareStatement("INSERT INTO a VALUES (false, ?, 1, 49, 29.255, 243434.432,'hey6',?,?,?)");
            ps.setShort(1,(short)23);
            Date da = Date.valueOf("2015-10-31");
            ps.setDate(2,da);
            Time t = Time.valueOf("14:11:29");
            ps.setTime(3,t);
            Timestamp ts = Timestamp.valueOf("2007-12-24 14:11:40");
            ps.setTimestamp(4,ts);
            ps.executeUpdate();
            int affected_rows = ps.getUpdateCount();
            System.out.println("Prepared statement update count: " + affected_rows + "\n");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void queryDBPreparedStatementDate (MonetConnection c) {
        try {
            System.out.println("Preparing statement (Date query)");
            //MonetPreparedStatement ps = (MonetPreparedStatement) c.prepareStatement("SELECT da, t, ts FROM a WHERE da = ? AND t = ? AND ts = ?");
            MonetPreparedStatement ps = (MonetPreparedStatement) c.prepareStatement("SELECT da, t, ts FROM a WHERE da <> ? AND t <> ? AND ts <> ?");
            Date da = Date.valueOf("2015-10-31");
            ps.setDate(1,da);
            Time t = Time.valueOf("14:11:29");
            ps.setTime(2,t);
            Timestamp ts = Timestamp.valueOf("2007-12-24 14:11:40");
            ps.setTimestamp(3,ts);
            ps.executeQuery();
            MonetResultSet rs = (MonetResultSet) ps.getResultSet();

            rs.beforeFirst();
            System.out.println("\nPrepared statement resultSet:");
            while (rs.next()) {
                System.out.println("Date: " + rs.getDate(0));
                System.out.println("Time: " + rs.getTime(1));
                System.out.println("Timestamp: " + rs.getTimestamp(2));
                System.out.println();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void queryDBLongQuery (MonetConnection c) {
        try {
            MonetStatement s = (MonetStatement) c.createStatement();
            s.executeQuery("SELECT sql_mul(i,s), radians(degrees(radians(i))), tan(i) FROM a;");
            MonetResultSet rs = (MonetResultSet) s.getResultSet();
            System.out.println("Select resultSet: ");
            rs.beforeFirst();
            while (rs.next()) {
                System.out.println("Row " + rs.getRow());
                System.out.println("Int: " + rs.getLong(0));
                System.out.println("Double: " + rs.getDouble(1));
                System.out.println("Double: " + rs.getDouble(2));
                System.out.println();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void blobPreparedQuery (MonetConnection c) {
        try {
            MonetPreparedStatement ps = (MonetPreparedStatement) c.prepareStatement("INSERT INTO b VALUES (?);");
            //ps.setBlob(1,new MonetBlob("12aa803F"));
            ps.setNull(1,Types.BLOB);
            int update_c = ps.executeUpdate();
            System.out.println("Update Count Prepared: " + update_c +"\n");

            MonetStatement s = (MonetStatement) c.createStatement();
            s.executeQuery("SELECT b FROM b;");
            MonetResultSet rs = (MonetResultSet) s.getResultSet();
            rs.beforeFirst();
            while (rs.next()) {
                System.out.println("Row " + rs.getRow());
                long blob_len = rs.getBlob(0).length();
                System.out.println("Blob length: " + blob_len);
                if (blob_len > 0) {
                    System.out.println("Blob first byte: " + rs.getBlob(0).getBytes(1,2)[0]);
                }
                else {
                    System.out.println("Null Blob");
                }
                System.out.println();
            }

            //Error here
            MonetPreparedStatement psSelect = (MonetPreparedStatement) c.prepareStatement("SELECT * from b WHERE b <> ?;");
            psSelect.setBlob(1,new MonetBlob("12aa803F"));
            psSelect.setBlob(1,new MonetBlob("12aa803F".getBytes()));
            MonetResultSet rsSelect = (MonetResultSet) psSelect.executeQuery();
            rsSelect.beforeFirst();
            System.out.println("rsp: " + rsSelect.getBlob(0).length());
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    //The Prepared Statement bind are not working (update count -1 but does not send error message)
    //The select is returning bad values for the literals inserted in the statement
    private static void int128Queries (MonetConnection c) {
        try {
            MonetStatement s = (MonetStatement) c.createStatement();
            System.out.println("\nCreate int128 table and insert rows");
            s.executeUpdate("CREATE TABLE big (bigi HUGEINT, bigd DECIMAL(16,8));");
            s.executeUpdate("INSERT INTO big VALUES " +
                    "(9323372036854775807,439.498)," +
                    "(9323372,38.2)," +
                    "(NULL,NULL);");
            System.out.println("Update Count Statement int128: " + s.getUpdateCount() +"\n");

            System.out.println("Insert into int128 table with prepared query");
            MonetPreparedStatement ps = (MonetPreparedStatement) c.prepareStatement("INSERT INTO big VALUES (940000000000,?);");
            //ps.setHugeInteger(1,new BigInteger("9400000000000000000"));
            ps.setBigDecimal(1,new BigDecimal(1328922).movePointLeft(5));
            System.out.println("Update Count Prepared int128: " + ps.getUpdateCount() +"\n");

            s.executeQuery("SELECT bigi,bigd FROM big;");
            MonetResultSet rs = (MonetResultSet) s.getResultSet();
            rs.beforeFirst();
            while (rs.next()) {
                System.out.println("Row " + rs.getRow());
                System.out.println("BigInteger: " + rs.getHugeInt(0));
                System.out.println("Decimal: " + rs.getBigDecimal(1));
                System.out.println();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void blobInsertQuery (MonetConnection c) {
        try {
            MonetStatement s = (MonetStatement) c.createStatement();

            System.out.println("\nCreate blob table");

            s.execute("CREATE TABLE b (b blob);");
            System.out.println("Insert into blob");
            s.execute("INSERT INTO b VALUES " +
                    "('12ff803F'), " +
                    "('0000803F')," +
                    "('12aa803F')," +
                    "('ffffffff');");
            System.out.println("Update Count Blob: " + s.getUpdateCount() +"\n");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void batchQueriesStatement(MonetConnection c) {
        try {
            MonetStatement s = (MonetStatement) c.createStatement();
            System.out.println("\nCreate batch table");
            s.execute("CREATE TABLE batch (b boolean)");

            System.out.println("Insert with executeBatch()");
            s.addBatch("INSERT INTO batch VALUES (false), (false);");
            s.addBatch("INSERT INTO batch VALUES (true), (true);");
            s.addBatch("INSERT INTO batch VALUES (false), (true), (false);");
            int[] updateCounts = s.executeBatch();
            for (int i = 0; i < updateCounts.length ; i++) {
                System.out.println("Batch query " + i + " with update count of " + updateCounts[i]);
                System.out.println();
            }

            System.out.println("Insert with executeLargeBatch()");
            s.addBatch("INSERT INTO batch VALUES (false);");
            s.addBatch("INSERT INTO batch VALUES (false), (true), (false), (true), (true);");
            long[] updateCountsLarge = s.executeLargeBatch();
            for (int i = 0; i < updateCountsLarge.length ; i++) {
                System.out.println("Large batch query " + i + " with update count of " + updateCountsLarge[i]);
                System.out.println();
            }

            s.executeQuery("SELECT b FROM batch;");
            MonetResultSet rs = (MonetResultSet) s.getResultSet();
            System.out.println("Inserted with batch: ");
            rs.beforeFirst();
            while (rs.next()) {
                System.out.println("Row " + rs.getRow());
                System.out.println("Bool: " + rs.getBoolean(0));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void batchQueriesPreparedStatement(MonetConnection c) {
        try {
            MonetStatement s = (MonetStatement) c.createStatement();
            System.out.println("\nCreate batch table");
            s.execute("CREATE TABLE batch (b boolean)");

            System.out.println("Insert with PreparedStatement executeBatch()");
            MonetPreparedStatement ps = (MonetPreparedStatement) c.prepareStatement("INSERT INTO batch VALUES (?), (?)");
            ps.setBoolean(1,false);
            ps.setBoolean(2,false);
            ps.addBatch();
            ps.setBoolean(1,true);
            ps.setBoolean(2,true);
            ps.addBatch();

            System.out.println("Execute Batch");
            int[] updateCounts =  ps.executeBatch();
            for (int i = 0; i < updateCounts.length ; i++) {
                System.out.println("Prepared Batch query " + i + " with update count of " + updateCounts[i]);
                System.out.println();
            }

            s.executeQuery("SELECT b FROM batch;");
            MonetResultSet rs = (MonetResultSet) s.getResultSet();
            System.out.println("Inserted with prepared batch: ");
            rs.beforeFirst();
            while (rs.next()) {
                System.out.println("Row " + rs.getRow());
                System.out.println("Bool: " + rs.getBoolean(0));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void transactionTest (MonetConnection c) {
        try {
            MonetStatement s = (MonetStatement) c.createStatement();
            s.executeUpdate("CREATE TABLE t (i int);");

            System.out.println("\nGet autocommit initial value: " + c.getAutoCommit());
            c.setAutoCommit(false);
            System.out.println("Set autocommit to false, current value: " + c.getAutoCommit() + "\n");


            s.executeUpdate("INSERT INTO t VALUES (12345);");
            c.rollback();

            MonetResultSet rs = (MonetResultSet) s.executeQuery("SELECT * FROM t;");
            rs.first();
            try {
                System.out.println("ResultSet from non-commited table (should not return anything?): " + rs.getObject(0));
            } catch (SQLException e) {
                System.out.println("ResultSet from non-commited table did not return anything");
            }

            //c.rollback();
            c.commit();

            rs = (MonetResultSet) s.executeQuery("SELECT * FROM t;");
            rs.first();
            try {
                System.out.println("ResultSet from commited table (should return tuple): " + rs.getObject(0) + "\n");
            } catch (SQLException e) {
                System.out.println("ResultSet from commited table did not return anything\n");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        try {
            Properties info = new Properties();

            //Memory DB
            String urlMemory = "jdbc:monetdb://:memory:";
            //Local DB
            String urlLocal = "jdbc:monetdb://localhost:5000/Users/bernardo/Monet/test/";
            //Proxy DB
            //String urlProxy = "mapi:monetdb://localhost:5000?database=test";
            String urlProxy = "mapi:monetdb://localhost:50000/test";

            String url = urlMemory;

            //Timeout properties
            info.setProperty("sessiontimeout","1");
            info.setProperty("querytimeout","1");

            Connection conn = DriverManager.getConnection(url, info);
            MonetConnection c = (MonetConnection) conn;

            if (c != null) {
                System.out.println("Opened connection @ " + url.substring(15));
                System.out.println("Query timeout is " + c.getClientInfo("querytimeout"));

                //Create and populate
                populateDBTable(c);

                //Prepared statements
                //queryDBPreparedStatement(c);
                //queryDBPreparedStatementDate(c);
                //insertDBPreparedStatementDate(c);
                //insertDBPreparedStatementNulls(c);

                //Query and drop
                queryDBStatement(c);
                dropDBTable(c);

                //Complex types tests
                //blobInsertQuery(c);
                //blobPreparedQuery(c);
                int128Queries(c);

                //Batch tests
                //batchQueriesStatement(c);
                //batchQueriesPreparedStatement(c);

                //Transaction and autocommit
                //transactionTest(c);

                c.close();
                System.out.println("Closed connection");
            } else {
                System.out.println("No connection was made");
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
