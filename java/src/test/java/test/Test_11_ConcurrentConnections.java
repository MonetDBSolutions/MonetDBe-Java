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

// TODO: Remove comments from concurrent connections when more than one active database are supported (future feature)
// TODO: Currently this only tests concurrent connections to one database
public class Test_11_ConcurrentConnections {

	@Test
	public void concurrentConnections() {
		
		Connection conn2 = null;
		Connection conn3 = null;
		
		try (Connection conn1 = DriverManager.getConnection(AllTests.LOCAL_CONNECTION, null)) {

			assertNotNull("Could not connect to database with connection string: " + AllTests.LOCAL_CONNECTION,
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
			conn2 = DriverManager.getConnection(AllTests.LOCAL_CONNECTION, null);

			assertNotNull("Could not connect to database with connection string: " + AllTests.LOCAL_CONNECTION,
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
			// TODO: This is a plan for future versions, not allowed right now
			/*conn3 = DriverManager.getConnection(AllTests.MEMORY_CONNECTION, null);

			assertNotNull("Could not connect to database with connection string: " + AllTests.MEMORY_CONNECTION,
					conn3);*/
			assertFalse(conn1.isClosed());
			assertFalse(conn2.isClosed());
			/*assertFalse(conn3.isClosed());

			try (Statement s = conn3.createStatement()) {
				s.executeUpdate("CREATE TABLE test11 (i INTEGER);");
				s.executeUpdate("INSERT INTO test11 VALUES (30), (40);");

				try (ResultSet rs = s.executeQuery("SELECT * FROM test11;")) {
					assertTrue(rs.next());
					assertEquals(30, rs.getInt(1));
					assertTrue(rs.next());
					assertEquals(40, rs.getInt(1));
				}
			}*/

			// Connection 1 can still be used
			try (Statement s = conn1.createStatement(); ResultSet rs = s.executeQuery("SELECT sum(i) FROM test11;")) {
				assertTrue(rs.next());
				assertEquals(3, rs.getInt(1));
			}

		} catch (SQLException e) {

			fail(e.toString());

		} finally {
			try {
				if (conn2 != null) {
					try (Statement s = conn2.createStatement()) {
						s.execute("DROP TABLE test11;");
					}
					conn2.close();
					assertTrue(conn2.isClosed());
				}
				if (conn3 != null) {
					try (Statement s = conn3.createStatement()) {
						s.execute("DROP TABLE test11;");
					}
					conn3.close();
					assertTrue(conn3.isClosed());
				}
			} catch (SQLException e) {
				fail(e.toString());
			}
		}
	}
}