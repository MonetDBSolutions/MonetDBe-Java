import java.sql.*;

public class Movies {
    private static void loadMovies(Connection conn) throws SQLException {
        String[] titles = {"Iron Man", "The Incredible Hulk", "Iron Man 2", "Thor", "Captain America: The First Avenger", "The Avengers", "Iron Man 3", "Captain America: The Winter Soldier", "Avengers: Age of Ultron", "Captain America: Civil War", "Doctor Strange", "Black Panther", "Avengers: Infinity War"};
        int[] years = {2008, 2008, 2010, 2011, 2011, 2012, 2013, 2014, 2015, 2016, 2016, 2018, 2018};

        //Using a Prepared Statement, we can reuse a query for multiple parameters
        PreparedStatement ps = conn.prepareStatement("INSERT INTO Movies (title, \"year\") VALUES (?,?)");

        for (int i = 0; i < titles.length; i++) {
            ps.setString(1, titles[i]);
            ps.setInt(2, years[i]);
            //Using addBatch, we can store multiple input parameters, which can then be executed at once
            ps.addBatch();
        }
        //Inserts all batched movies
        ps.executeBatch();
    }

    private static void queryMovies(Statement s) throws SQLException {
        ResultSet rs = s.executeQuery("SELECT * FROM Movies;");
        while (rs.next()) {
            System.out.println("Id: " + rs.getInt(1) + " / Title: " + rs.getString(2) + " / Year: " + rs.getInt(3));
        }

        rs = s.executeQuery("SELECT * FROM Movies ORDER BY \"year\" DESC;");
        while (rs.next()) {
            System.out.println("Id: " + rs.getInt(1) + " / Title: " + rs.getString(2) + " / Year: " + rs.getInt(3));
        }
    }

    private static void loadActors(Connection conn) throws SQLException {
        String[] first_name = {"Robert","Chris","Scarlett","Samuel L.","Benedict","Brie","Chadwick"};
        String[] last_name = {"Downey Jr.", "Evans", "Johansson", "Jackson", "Cumberbatch", "Larson", "Boseman"};
        String[] character = {"Iron Man", "Captain America", "Black Widow", "Nick Fury", "Dr. Strange", "Captain Marvel", "Black Panther"};
        int[] age = {53, 37, 33, 69, 42, 29, 40};

        PreparedStatement ps = conn.prepareStatement("INSERT INTO Actors (first_name, last_name, \"character\", age) VALUES (?,?,?,?);");

        for (int i = 0; i < age.length; i++) {
            ps.setString(1, first_name[i]);
            ps.setString(2, last_name[i]);
            ps.setString(3, character[i]);
            ps.setInt(4, age[i]);
            ps.addBatch();
        }
        ps.executeBatch();

        int[] actors = {1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 4, 4, 4, 4, 4, 4, 4, 4, 5, 5, 7, 7, 7};
        int[] movies = {1, 2, 3, 6, 7, 9, 10, 13, 5, 6, 8, 9, 10, 13, 3, 6, 8, 9, 10, 13, 1, 3, 4, 5, 6, 8, 9, 13, 11, 13, 10, 12, 13};
        ps = conn.prepareStatement("INSERT INTO MovieActors (movie_id, actor_id) VALUES (?,?);");

        for (int i = 0; i < actors.length; i++) {
            ps.setInt(1, movies[i]);
            ps.setInt(2, actors[i]);
            ps.addBatch();
        }
        ps.executeBatch();
    }

    private static void queryActorMovies(Statement s) throws SQLException {
        ResultSet rs = s.executeQuery("SELECT Movies.title, Movies.\"year\", Actors.first_name, Actors.\"character\" FROM MovieActors JOIN Movies ON MovieActors.movie_id = Movies.id JOIN Actors ON MovieActors.actor_id = Actors.id;");
        while (rs.next()) {
            System.out.println("Movie Title: " + rs.getString(1) + " / Movie Year: " + rs.getInt(2) + " / Actor First Name: " + rs.getString(3) + " / Actor Character: " + rs.getString(4));
        }
    }

    public static void main(String[] args) {
        String db = "/tmp/movies.mdbe";
        Connection conn = null;
        try {
            //Local database
            conn = DriverManager.getConnection("jdbc:monetdb:file:" + db, null);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        if (conn == null) {
            System.out.println("Could not connect to local database");
            return;
        }

        try {
            Statement s = conn.createStatement();

            //Here we create a primary key, and use "NOT NULL" to prevent inserting invalid data
            s.executeUpdate("CREATE TABLE Movies (id SERIAL, title STRING NOT NULL, \"year\" INTEGER NOT NULL);");
            loadMovies(conn);
            queryMovies(s);

            s.executeUpdate("CREATE TABLE Actors (id SERIAL, first_name TEXT NOT NULL, last_name TEXT NOT NULL, \"character\" TEXT NOT NULL, age REAL NOT NULL);");
            s.executeUpdate("CREATE TABLE MovieActors (id SERIAL, movie_id INTEGER NOT NULL, actor_id INTEGER NOT NULL);");
            loadActors(conn);

            queryActorMovies(s);

            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
