import nl.cwi.monetdb.monetdbe.MonetDriver;
import nl.cwi.monetdb.monetdbe.MonetConnection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class TestMonetDBeJava {
    static {
        try {
            Class.forName("nl.cwi.monetdb.monetdbe.MonetDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static void main (String[] args) {
        try {
            Connection conn = DriverManager.getConnection("jdbc:monetdb://localhost/test");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
