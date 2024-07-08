package test;

import org.junit.Test;

import java.sql.*;
import java.util.Properties;
import java.util.stream.Stream;

import static org.junit.Assert.*;

public class Test_21_ConnectionOptions {
    @Test
    public void connectionOptions() {
        Stream.of(AllTests.CONNECTIONS).forEach(this::connectionOptions);
    }

    private void sessionTimeout (String connectionUrl, Integer shortTimeout, Integer longTimeout) {
        Properties props = new Properties();
        props.setProperty("session_timeout",shortTimeout.toString());
        long start = System.currentTimeMillis();
        long duration;
        //Session timeout: timeout reached case
        try (Connection conn = DriverManager.getConnection(connectionUrl, props)) {
            Statement st = conn.createStatement();
            Thread.sleep(shortTimeout);
            st.executeQuery("select 1;");
            fail("Timeout not reached");
        } catch (SQLException e) {
            duration = System.currentTimeMillis() - start;
            assertEquals(e.toString(),"java.sql.SQLException: MALException:mal.interpreter:HYT00!Query aborted due to session timeout");
            assertTrue(duration > shortTimeout);
        } catch (InterruptedException e) {
            fail(e.toString());
        }
        //Session timeout: timeout not reached case
        duration = 0;
        props = new Properties();
        props.setProperty("session_timeout",longTimeout.toString());
        start = System.currentTimeMillis();
        try (Connection conn = DriverManager.getConnection(connectionUrl, props)) {
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery("select 1;");
            conn.close();
            duration = System.currentTimeMillis() - start;
            assertNotNull(rs);
            assertTrue(duration < longTimeout);
        } catch (SQLException e) {
            if (duration == 0)
                duration = System.currentTimeMillis() - start;
            fail(e.toString() + "\nTimeout: " + longTimeout + " \tExecution time: " + duration);
        }
    }

    private void queryTimeout (String connectionUrl, Integer shortTimeout, Integer longTimeout) {
        Properties props = new Properties();
        props.setProperty("query_timeout",shortTimeout.toString());
        long start = 0;
        long duration = 0;
        //Query timeout: timeout reached case
        try (Connection conn = DriverManager.getConnection(connectionUrl, props)) {
            Statement st = conn.createStatement();
            //Wait longer than the short timeout, so we know that query timeout is acting different from session timeout
            Thread.sleep(1000);
            start = System.currentTimeMillis();
            st.executeQuery("select 1;");
            fail("Timeout not reached");
        } catch (SQLException e) {
            duration = System.currentTimeMillis() - start;
            assertEquals(e.toString(),"java.sql.SQLException: MALException:mal.interpreter:HYT00!Query aborted due to timeout");
            assertTrue(duration > shortTimeout);
        } catch (InterruptedException e) {
            fail(e.toString());
        }
        //Query timeout: timeout not reached case
        duration = 0;
        props = new Properties();
        props.setProperty("query_timeout",longTimeout.toString());
        try (Connection conn = DriverManager.getConnection(connectionUrl, props)) {
            Statement st = conn.createStatement();
            //Wait longer than the long timeout, so we know that query timeout is acting different from session timeout
            Thread.sleep(1000);
            start = System.currentTimeMillis();
            ResultSet rs = st.executeQuery("select 1;");
            duration = System.currentTimeMillis() - start;
            assertNotNull(rs);
            assertTrue(duration < longTimeout);
        } catch (SQLException e) {
            fail(e.toString() + "\nTimeout: " + longTimeout + " \tExecution time: " + duration);
        } catch (InterruptedException e) {
            fail(e.toString());
        }
        //Query timeout: Test if session timeout fails faster than query timeout
        props = new Properties();
        props.setProperty("query_timeout",longTimeout.toString());
        props.setProperty("session_timeout",longTimeout.toString());
        try (Connection conn = DriverManager.getConnection(connectionUrl, props)) {
            Statement st = conn.createStatement();
            st.executeQuery("select 1;");
            fail("Timeout not reached");
        } catch (SQLException e) {
            assertEquals(e.toString(),"java.sql.SQLException: MALException:mal.interpreter:HYT00!Query aborted due to session timeout");
        }
    }

    private void checkSessionValues (String connectionUrl, Integer memoryLimit, Integer threadsLimit, Integer timeout) {
        Properties props = new Properties();
        props.setProperty("memory_limit",memoryLimit.toString());
        props.setProperty("nr_threads",threadsLimit.toString());
        props.setProperty("session_timeout",timeout.toString());
        props.setProperty("query_timeout",timeout.toString());
        try (Connection conn = DriverManager.getConnection(connectionUrl, props)) {
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery("select name, value FROM sys.session() where name in ('sessiontimeout','querytimeout','workerlimit','memorylimit');");
            while(rs.next()) {
                String name = rs.getString(1);
                int value = rs.getInt(2);
                if (name.equals("sessiontimeout") || name.equals("querytimeout"))
                    assertEquals(value,(int) timeout);
                else if (name.equals("workerlimit"))
                    assertEquals(value,(int) threadsLimit);
                else if (name.equals("memorylimit"))
                    assertEquals(value,(int) memoryLimit);
                else
                    fail("Unknown variable");
            }
        } catch (SQLException e) {
            fail(e.toString());
        }
    }

    private void connectionOptions(String connectionUrl) {
        sessionTimeout(connectionUrl, 100, 2000);
        queryTimeout(connectionUrl, 1, 100);
        //checkSessionValues(connectionUrl,1024,2,10000);
    }
}
