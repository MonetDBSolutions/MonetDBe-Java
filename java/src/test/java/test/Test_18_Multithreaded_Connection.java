package test;

import org.junit.Test;
import org.monetdb.monetdbe.MonetResultSet;

import java.math.BigDecimal;
import java.sql.*;
import java.util.Calendar;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;


class MultiThreadRun implements Runnable {
    String tableName;
    int threadNum;
    Connection c;

    public MultiThreadRun(Connection c, int threadNum) {
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
            assertEquals(1,stat.executeUpdate("INSERT INTO " + tableName + " VALUES (" + threadNum + ")"));
            rs = stat.executeQuery("SELECT * FROM " + tableName);
            assertEquals(1,((MonetResultSet) rs).getRowsNumber());
            while (rs.next())
                assertEquals(threadNum,rs.getObject(1));
            rs = stat.executeQuery("SELECT * FROM  a_0");
            assertEquals(1,((MonetResultSet) rs).getRowsNumber());
            while(rs.next())
                assertEquals(0,rs.getObject(1));
            assertFalse(rs.next());
            c.close();
        } catch (SQLException e) {
            fail(e.toString());
        }

    }
}

public class Test_18_Multithreaded_Connection {
    @Test
    public void multithreadedConnection() {
        Stream.of(AllTests.CONNECTIONS).forEach(this::multithreadedConnection);
    }

    private void multithreadedConnection(String connectionUrl) {
        try (Connection conn = DriverManager.getConnection(connectionUrl, null)) {
            int n = 5;
            ExecutorService executor = Executors.newFixedThreadPool(n);
            for (int i = 0; i < n; i++) {
                Runnable t = null;
                t = new MultiThreadRun(conn, i);
                executor.execute(t);
            }
            executor.shutdown();
            try {
                executor.awaitTermination(10,TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            Statement dropStat = conn.createStatement();
            for (int i = 0; i < n; i++) {
                //Cleanup
                int result = dropStat.executeUpdate("DROP TABLE a_" + i + ";");
                assertEquals(result,0);
            }
            try {
                conn.close();
                assertTrue(conn.isClosed());
            } catch (SQLException e) {
                e.printStackTrace();
            }
        } catch (SQLException e) {
            fail(e.toString());
        }
    }
}
