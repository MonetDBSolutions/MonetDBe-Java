import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class HelloWorld {
    public static void main(String[] args) {
        Connection conn = null;
        try {
            //In-memory database
            conn = DriverManager.getConnection("jdbc:monetdb:memory:",null);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        if (conn == null) {
            System.out.println("Could not connect to memory database");
            return;
        }

        System.out.println("Hello world, we have a lift off!\nMonetDB/e has been started");

        try {
            //Close connection
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        System.out.println("Hello World, we have safely returned");
    }
}
