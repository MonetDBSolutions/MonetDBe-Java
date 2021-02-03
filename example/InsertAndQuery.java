import java.sql.*;

public class InsertAndQuery {
    public static void main(String[] args) {
        Connection conn = null;
        try {
            //In-memory database
            conn = DriverManager.getConnection("jdbc:monetdb://:memory:",null);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        if (conn == null) {
            System.out.println("Could not connect to memory database");
            return;
        }


        try {
            //Create table and insert values
            Statement s = conn.createStatement();
            System.out.println("Creating table and inserting values.");
            s.executeUpdate("CREATE TABLE example (b BOOLEAN, i INTEGER, s STRING);");
            s.executeUpdate("INSERT INTO example VALUES (false,3,'hello'), (true,500,'world'),(false,-1,NULL);");

            //Query table
            ResultSet rs = s.executeQuery("SELECT * FROM example;");

            System.out.println("Fetched values:");
            //Fetch results
            while (rs.next()) {
                System.out.println("Bool: " + rs.getBoolean(1));
                System.out.println("Int: " + rs.getInt(2));
                System.out.println("String: " + rs.getString(3) + "\n");
            }

            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
