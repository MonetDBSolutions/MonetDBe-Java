package test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.stream.Stream;

import org.junit.Test;

public class Test_02_CloseConnectionTwice {

	@Test
	public void CloseConnectionTwice() {
		Stream.of(Configuration.CONNECTIONS).forEach(x -> CloseConnectionTwice(x));
	}

	private void CloseConnectionTwice(String connectionUrl) {
		try {

			@SuppressWarnings("resource")
			Connection conn = DriverManager.getConnection(connectionUrl, null);

			assertNotNull("Could not connect to database with connection string: " + connectionUrl, conn);
			assertFalse(conn.isClosed());

			// MonetDB/e connection opened successfully

			conn.close();
			assertTrue(conn.isClosed());

			// MonetDB/e connection closed successfully

			conn.close();
			assertTrue(conn.isClosed());

		} catch (SQLException e) {

			fail(e.toString());

		}
	}
}