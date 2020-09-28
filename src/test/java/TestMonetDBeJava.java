import nl.cwi.monetdb.monetdbe.MonetDriver;
import nl.cwi.monetdb.monetdbe.MonetConnection;

import org.duckdb.DuckDBDriver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class TestMonetDBeJava {
    static {
        try {
            //Class.forName("nl.cwi.monetdb.monetdbe.MonetDriver");
            Class.forName("org.duckdb.DuckDBDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static void main (String[] args) {
        try {
            System.out.println(DriverManager.getDrivers().nextElement().toString());
            //Connection conn = DriverManager.getConnection("jdbc:monetdb://localhost/test");
            Connection conn = DriverManager.getConnection("jdbc:duckdb://localhost/test");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
