package test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Test;

public class Test_11_ConcurrentConnections {

	@Test
	public void openAndCloseConnection() {
		try {

			@SuppressWarnings("resource")
			Connection conn1 = DriverManager.getConnection(Configuration.LOCAL_CONNECTION, null);

			assertNotNull("Could not connect to database with connection string: " + Configuration.LOCAL_CONNECTION,
					conn1);
			assertFalse(conn1.isClosed());

			try (Statement s = conn1.createStatement()) {
				s.executeUpdate("CREATE TABLE test11 (i INTEGER);");
				s.executeUpdate("INSERT INTO test11 VALUES (1), (2);");

				try (ResultSet rs = s.executeQuery("SELECT * FROM test11;")) {
					assertTrue(rs.next());
					assertEquals(1, rs.getInt(1));
					assertTrue(rs.next());
					assertEquals(2, rs.getInt(1));
				}
			}

			// Connection to same database
			@SuppressWarnings("resource")
			Connection conn2 = DriverManager.getConnection(Configuration.LOCAL_CONNECTION, null);

			assertNotNull("Could not connect to database with connection string: " + Configuration.LOCAL_CONNECTION,
					conn2);
			assertFalse(conn1.isClosed());
			assertFalse(conn2.isClosed());

			try (Statement s = conn2.createStatement(); ResultSet rs = s.executeQuery("SELECT * FROM test11;")) {
				assertTrue(rs.next());
				assertEquals(1, rs.getInt(1));
				assertTrue(rs.next());
				assertEquals(2, rs.getInt(1));
			}

			// Connection 1 can still be used
			try (Statement s = conn1.createStatement(); ResultSet rs = s.executeQuery("SELECT sum(i) FROM test11;")) {
				assertTrue(rs.next());
				assertEquals(3, rs.getInt(1));
			}

			// Connecting to another database
			// TODO: Allow multiple concurrent connections
			@SuppressWarnings("resource")
			Connection conn3 = DriverManager.getConnection(Configuration.MEMORY_CONNECTION, null);

			assertNotNull("Could not connect to database with connection string: " + Configuration.MEMORY_CONNECTION,
					conn3);
			assertFalse(conn1.isClosed());
			assertFalse(conn2.isClosed());
			assertFalse(conn3.isClosed());

			try (Statement s = conn3.createStatement()) {
				s.executeUpdate("CREATE TABLE test11 (i INTEGER);");
				s.executeUpdate("INSERT INTO test11 VALUES (30), (40);");

				try (ResultSet rs = s.executeQuery("SELECT * FROM test11;")) {
					assertTrue(rs.next());
					assertEquals(30, rs.getInt(1));
					assertTrue(rs.next());
					assertEquals(40, rs.getInt(1));
				}
			}

			// Connection 1 can still be used
			try (Statement s = conn1.createStatement(); ResultSet rs = s.executeQuery("SELECT sum(i) FROM test11;")) {
				assertTrue(rs.next());
				assertEquals(3, rs.getInt(1));
			}

			try (Statement s1 = conn1.createStatement(); Statement s3 = conn3.createStatement()) {
				s1.execute("DROP TABLE test11;");
				s3.execute("DROP TABLE test11;");
			}

			conn1.close();
			conn2.close();
			conn3.close();
			assertTrue(conn1.isClosed());
			assertTrue(conn2.isClosed());
			assertTrue(conn3.isClosed());

			// MonetDB/e connections closed successfully

		} catch (SQLException e) {

			fail(e.toString());

		}
	}
}