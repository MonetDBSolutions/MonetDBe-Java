package test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.stream.Stream;

import org.junit.Test;
import org.monetdb.monetdbe.MonetResultSet;

public class Test_12_BatchesAndJoinsMovies {

	private final String[] TITLES = {"Iron Man", "The Incredible Hulk", "Iron Man 2", "Thor", "Captain America: The First Avenger", "The Avengers", "Iron Man 3", "Captain America: The Winter Soldier", "Avengers: Age of Ultron", "Captain America: Civil War", "Doctor Strange", "Black Panther", "Avengers: Infinity War"};
	private final int[] YEARS = {2008, 2008, 2010, 2011, 2011, 2012, 2013, 2014, 2015, 2016, 2016, 2018, 2018};
	private final String[] FIRSTNAME = {"Robert","Chris","Scarlett","Samuel L.","Benedict","Brie","Chadwick"};
	private final String[] LASTNAME = {"Downey Jr.", "Evans", "Johansson", "Jackson", "Cumberbatch", "Larson", "Boseman"};
	private final String[] HERO = {"Iron Man", "Captain America", "Black Widow", "Nick Fury", "Dr. Strange", "Captain Marvel", "Black Panther"};
	private final int[] AGE = {53, 37, 33, 69, 42, 29, 40};
	private final int[] ACTORS = {1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 4, 4, 4, 4, 4, 4, 4, 4, 5, 5, 7, 7, 7};
	private final int[] MOVIES = {1, 2, 3, 6, 7, 9, 10, 13, 5, 6, 8, 9, 10, 13, 3, 6, 8, 9, 10, 13, 1, 3, 4, 5, 6, 8, 9, 13, 11, 13, 10, 12, 13};
    
	@Test
	public void batchesAndJoinsMovies() {
		Stream.of(AllTests.CONNECTIONS).forEach(x -> batchesAndJoinsMovies(x));
	}

	private void batchesAndJoinsMovies(String connectionUrl) {
		try {

			try (Connection conn = DriverManager.getConnection(connectionUrl, null)) {
				assertNotNull("Could not connect to database with connection string: " + connectionUrl, conn);
				assertFalse(conn.isClosed());
				assertTrue(conn.getAutoCommit());

				// Here we create a primary key, and use "NOT NULL" to prevent inserting invalid data
				try (Statement statement = conn.createStatement()) {
					statement.executeUpdate("CREATE TABLE Movies (id SERIAL, title STRING NOT NULL, \"year\" INTEGER NOT NULL);");
					statement.executeUpdate("CREATE TABLE Actors (id SERIAL, first_name TEXT NOT NULL, last_name TEXT NOT NULL, \"character\" TEXT NOT NULL, age REAL NOT NULL);");
					statement.executeUpdate("CREATE TABLE MovieActors (id SERIAL, movie_id INTEGER NOT NULL, actor_id INTEGER NOT NULL);");
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
		        	assertEquals(TITLES.length, ((MonetResultSet) rs).getRowsNumber());
					assertEquals(3, ((MonetResultSet) rs).getColumnsNumber());
		        }
		        try (Statement statement = conn.createStatement();
		        		ResultSet rs = statement.executeQuery("SELECT * FROM Movies ORDER BY \"year\" DESC;")) {
		        	assertEquals(TITLES.length, ((MonetResultSet) rs).getRowsNumber());
					assertEquals(3, ((MonetResultSet) rs).getColumnsNumber());
		        }

		        try (Statement statement = conn.createStatement();
		        		ResultSet rs = statement.executeQuery("SELECT Movies.title, Movies.\"year\", Actors.first_name, Actors.\"character\" FROM MovieActors JOIN Movies ON MovieActors.movie_id = Movies.id JOIN Actors ON MovieActors.actor_id = Actors.id;")) {
		        	assertEquals(33, ((MonetResultSet) rs).getRowsNumber());
					assertEquals(4, ((MonetResultSet) rs).getColumnsNumber());
		        }
		        
		        // Clean up
		        try (Statement statement = conn.createStatement()) {
					//TODO Figure out why this returns -> SQLException:sql.drop_table:42000!DROP TABLE: unable to drop table movieactors (there are database objects which depend on it)
					//statement.executeUpdate("DROP TABLE MovieActors;");
		        	statement.executeUpdate("DROP TABLE Actors;");
		        	statement.executeUpdate("DROP TABLE Movies;");
		        }
			}
			
		} catch (SQLException e) {

			fail(e.toString());

		}
	}
}