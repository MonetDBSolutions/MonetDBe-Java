package test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Test;
import org.monetdb.monetdbe.MonetResultSet;

public class Test_09_LoadAndQueryTaxi {

	private static final int ROW_COUNT = 10906858;

	@Test
	public void loadAndQueryTaxi() {
		// Stream.of(Configuration.CONNECTIONS).forEach(x -> loadAndQueryTaxi(x));
		loadAndQueryTaxi(Configuration.MEMORY_CONNECTION);
	}

	private void loadAndQueryTaxi(String connectionUrl) {
		try (Connection conn = DriverManager.getConnection(connectionUrl, null)) {

			assertNotNull("Could not connect to database with connection string: " + connectionUrl, conn);
			assertFalse(conn.isClosed());
			assertTrue(conn.getAutoCommit());

			// Create table and insert values
			try (Statement statement = conn.createStatement()) {
				statement.executeUpdate("CREATE TABLE test09 (" +
					"VendorID BIGINT," +
					"tpep_pickup_datetime TIMESTAMP," +
					"tpep_dropoff_datetime TIMESTAMP," +
					"passenger_count BIGINT," +
					"trip_distance DOUBLE," +
					"pickup_longitude DOUBLE," +
					"pickup_latitude DOUBLE," +
					"RatecodeID BIGINT," +
					"store_and_fwd_flag STRING," +
					"dropoff_longitude DOUBLE," +
					"dropoff_latitude DOUBLE," +
					"payment_type BIGINT," +
					"fare_amount DOUBLE," +
					"extra DOUBLE," +
					"mta_tax DOUBLE," +
					"tip_amount DOUBLE," +
					"tolls_amount DOUBLE," +
					"improvement_surcharge DOUBLE," +
					"total_amount DOUBLE);");
			}
			
			try (Statement statement = conn.createStatement()) {
				try {
					File raw = new File(Configuration.TAXI_CSV);
					File canonicalFile = raw.isAbsolute()
							? raw.getCanonicalFile()
							: new File(new File("."), Configuration.TAXI_CSV).getCanonicalFile();
					assertTrue(canonicalFile.exists());
					assertTrue(canonicalFile.isFile());
					assertTrue(canonicalFile.canRead());
					
					String canonicalFileLocation = canonicalFile.getPath().replace('\\', '/');
					
					long rows = statement.executeLargeUpdate("COPY OFFSET 2 INTO test09 FROM '" +
							canonicalFileLocation + "' DELIMITERS ',', '\\n' BEST EFFORT;");
					assertEquals(ROW_COUNT, rows);
					
				} catch (IOException e) {
					fail("File " + Configuration.TAXI_CSV + " cannot be found: " + e.toString());
				}
			}
			
			try (Statement statement = conn.createStatement();
				ResultSet distinct = statement.executeQuery("SELECT " +
	                "COUNT(DISTINCT VendorID)," +
	                "COUNT(DISTINCT passenger_count)," +
	                "COUNT(DISTINCT trip_distance)," +
	                "COUNT(DISTINCT RatecodeID)," +
	                "COUNT(DISTINCT store_and_fwd_flag)," +
	                "COUNT(DISTINCT payment_type)," +
	                "COUNT(DISTINCT fare_amount)," +
	                "COUNT(DISTINCT extra)," +
	                "COUNT(DISTINCT mta_tax)," +
	                "COUNT(DISTINCT tip_amount)," +
	                "COUNT(DISTINCT tolls_amount)," +
	                "COUNT(DISTINCT improvement_surcharge)," +
	                "COUNT(DISTINCT total_amount) " +
	                "FROM test09;")) {
				assertEquals(1, ((MonetResultSet) distinct).getRowsNumber());
				
				distinct.next();
				assertEquals(2, distinct.getInt(1));
				assertEquals(10, distinct.getInt(2));
				assertEquals(4513, distinct.getInt(3));
				assertEquals(7, distinct.getInt(4));
				assertEquals(2, distinct.getInt(5));
				assertEquals(5, distinct.getInt(6));
				assertEquals(1878, distinct.getInt(7));
				assertEquals(35, distinct.getInt(8));
				assertEquals(16, distinct.getInt(9));
				assertEquals(3551, distinct.getInt(10));
				assertEquals(940, distinct.getInt(11));
				assertEquals(7, distinct.getInt(12));
				assertEquals(11166, distinct.getInt(13));
			}
			
			try (Statement statement = conn.createStatement();
				ResultSet frequency = statement.executeQuery("SELECT MIN(cnt), AVG(cnt), MEDIAN(cnt), MAX(cnt) " +
                    "FROM" +
                    "(" +
                    "    SELECT " +
                    "        COUNT(*) as cnt" +
                    "    FROM test09" +
                    "    GROUP BY  " +
                    "        EXTRACT(DOY FROM tpep_pickup_datetime)," +
                    "        EXTRACT(HOUR FROM tpep_pickup_datetime)" +
                    ") stats;")) {
				assertEquals(1, ((MonetResultSet) frequency).getRowsNumber());
				
				frequency.next();
				assertEquals(8, frequency.getInt(1));
				assertEquals(14659.755d, frequency.getDouble(2), .01d);
				assertEquals(16899.0d, frequency.getDouble(3), .01d);
				assertEquals(28511, frequency.getInt(4));	
			}
				
			try (Statement statement = conn.createStatement();
				ResultSet max = statement.executeQuery("SELECT " +
                    "AVG(fare_amount) + 3 * STDDEV_SAMP(fare_amount) as max_fare," +
                    "AVG(trip_distance) + 3 * STDDEV_SAMP(trip_distance) as max_distance " +
                    "FROM test09;")) {

				assertEquals(1, ((MonetResultSet) max).getRowsNumber());
				
				max.next();
	            double max_fare = max.getDouble(1);
	            double max_distance = max.getDouble(2);
	            assertEquals(119.178d, max_fare, .01d);
				assertEquals(8947.934d, max_distance, .01d);

	            try (ResultSet average = statement.executeQuery("SELECT " +
                    "(SUM(trip_distance * fare_amount) - SUM(trip_distance) * SUM(fare_amount) / COUNT(*)) / " +
                    "(SUM(trip_distance * trip_distance) - SUM(trip_distance) * SUM(trip_distance) / COUNT(*)) AS beta," +
                    "AVG(fare_amount) AS avg_fare_amount," +
                    "AVG(trip_distance) AS avg_trip_distance " +
                    "FROM test09 " +
                    "WHERE " +
                    "fare_amount > 0 AND " +
                    "fare_amount < " + max_fare + " AND " +
                    "trip_distance > 0 AND " +
                    "trip_distance < " + max_distance)) {

	            	assertEquals(1, ((MonetResultSet) average).getRowsNumber());
					
	            	average.next();
	                double beta = average.getDouble(1);
	                double avg_fare_amount = average.getDouble(2);
	                double avg_trip_distance = average.getDouble(3);
	                assertEquals(2.661d, beta, .01d);
	                assertEquals(12.389d, avg_fare_amount, .01d);
	                assertEquals(2.907d, avg_trip_distance, .01d);
	            }
			}
			
			// Clean up
			try (Statement statement = conn.createStatement()) {
				statement.executeUpdate("DROP TABLE test09;");

				assertEquals(ROW_COUNT, statement.getUpdateCount());
			}
			
		} catch(SQLException e) {

			fail(e.toString());
	
		}
	}
}