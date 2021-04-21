package test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.stream.Stream;

import org.junit.Test;

public class Test_03_AutoCommit {

	@Test
	public void autoCommit() {
		Stream.of(Configuration.CONNECTIONS).forEach(x -> {
			autoCommit(x, true);
			autoCommit(x + Configuration.AUTOCOMMIT_FALSE_PARM, false);
		});
	}

	private void autoCommit(String connectionUrl, boolean autoCommit) {
		try {

			@SuppressWarnings("resource")
			Connection conn = DriverManager.getConnection(connectionUrl, null);

			assertNotNull("Could not connect to database with connection string: " + connectionUrl, conn);
			assertFalse(conn.isClosed());

			// MonetDB/e connection opened successfully

			assertEquals(autoCommit, conn.getAutoCommit());

			conn.close();
			assertTrue(conn.isClosed());

			// MonetDB/e connection closed successfully

		} catch (SQLException e) {

			fail(e.toString());

		}
	}
}