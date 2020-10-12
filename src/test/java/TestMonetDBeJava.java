import nl.cwi.monetdb.monetdbe.MonetDriver;
import nl.cwi.monetdb.monetdbe.MonetConnection;
import nl.cwi.monetdb.monetdbe.MonetStatement;

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
        /*try {
            System.out.println(DriverManager.getDrivers().nextElement().toString());
            Connection conn = DriverManager.getConnection("jdbc:monetdb://localhost/test");
        } catch (SQLException e) {
            e.printStackTrace();
        }*/
        MonetDriver m = new MonetDriver();
        MonetConnection c;
        try {
            c = (MonetConnection) m.connect("jdbc:monetdb://localhost/home/bernardo/MonetDB-Jun2020/db-farm/test",null);
            //c = (MonetConnection) m.connect("jdbc:monetdb://localhost:memory:",null);
            if (c!= null) {
                MonetStatement s = (MonetStatement) c.createStatement();
                s.execute("CREATE TABLE test1(id int);");
                s.execute("INSERT INTO test1 VALUES (1), (2), (3);");
                s.execute("SELECT * FROM test1;");
                s.execute("DROP TABLE test1;");
                c.close();
            }
            else {
                System.out.println("No connection was made");
            }
        } catch (SQLException | NullPointerException e) {
            e.printStackTrace();
        }
    }
}
