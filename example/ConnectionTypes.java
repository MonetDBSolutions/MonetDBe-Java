import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ConnectionTypes {
    public static void main(String[] args) {
        //In-Memory DB
        String urlMemory = "jdbc:monetdb://:memory:";
        //Local Persistent DB
        String urlLocal = "jdbc:monetdb:/tmp/test/";
        //Remote Proxy DB (needs to have a server running)
        String urlProxy = "mapi:monetdb://localhost:50000/test";

        //Connecting to in-memory DB
        try {
            Connection memoryConnection = DriverManager.getConnection(urlMemory,null);

            if (memoryConnection != null) {
                System.out.println("Successful connection to in-memory database.");
                memoryConnection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("In-memory database connection failure.");
        }

        //Connecting to a local persistent DB
        try {
            Connection localConnection = DriverManager.getConnection(urlLocal,null);

            if (localConnection != null) {
                System.out.println("Successful connection to local persistent database.");
                localConnection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("Local persistent connection failure.");
        }

        //Connecting to a remote proxy DB
        try {
            Connection proxyConnection = DriverManager.getConnection(urlProxy,null);

            if (proxyConnection != null) {
                System.out.println("Successful connection to a remote proxy database.");
                proxyConnection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("Remote proxy database connection failure.");
        }
    }
}
