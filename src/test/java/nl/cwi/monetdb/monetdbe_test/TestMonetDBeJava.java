package nl.cwi.monetdb.monetdbe_test;

import nl.cwi.monetdb.monetdbe.*;

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

    public TestMonetDBeJava() {
        test();
    }

    public void test() {
        try {
            System.out.println(DriverManager.getDrivers().nextElement().toString());
            Connection conn = DriverManager.getConnection("jdbc:monetdb://localhost/test");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main (String[] args) {
        TestMonetDBeJava t = new TestMonetDBeJava();
    }
}
