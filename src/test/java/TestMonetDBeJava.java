import nl.cwi.monetdb.monetdbe.MonetDriver;
import nl.cwi.monetdb.monetdbe.MonetConnection;
import nl.cwi.monetdb.monetdbe.MonetResultSet;
import nl.cwi.monetdb.monetdbe.MonetStatement;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

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
        String url = ":memory:";
        Properties info = null;
        try {
            c = (MonetConnection) m.connect(url,info);
            if (c!= null) {
                System.out.println("Opened connection @ " + url);
                MonetStatement s = (MonetStatement) c.createStatement();

                System.out.println("Create table");
                //s.execute("CREATE TABLE a(id int, name string);");
                //s.execute("INSERT INTO a VALUES (1,'a'), (2,'b'), (3,'c');");
                //s.execute("CREATE TABLE a (b bool);");
                //s.execute("INSERT INTO a VALUES (TRUE), (TRUE), (FALSE);");
                /*s.execute("CREATE TABLE a(id int, b boolean, l float);");
                s.execute("INSERT INTO a VALUES (1,true,3.7), (2,false,2.98), (3,false,2.63), (4,true,1.0);");*/
                s.execute("CREATE TABLE a (b boolean, s smallint, i int, l bigint, f float, d double);");
                s.execute("INSERT INTO a VALUES (true, 2, 3, 5, 1.0, 1.66), (true, 4, 6, 10, 2.5, 3.643), (false, 8, 12, 20, 25.25, 372.325), (false, 16, 24, 40, 255.255, 2434.432);");
                System.out.println("Insert update count: " + s.getUpdateCount());
                s.execute("SELECT * FROM a;");
                MonetResultSet rs = (MonetResultSet) s.getResultSet();
                System.out.println("Select resultSet: ");
                rs.beforeFirst();
                while(rs.next()) {
                    System.out.println("Row " + rs.getRow());
                    System.out.println("Bool: " + rs.getBoolean(0));
                    System.out.println("Short: " + rs.getShort(1));
                    System.out.println("Int: " + rs.getInt(2));
                    System.out.println("Long: " + rs.getLong(3));
                    System.out.println("Float: " + rs.getFloat(4));
                    System.out.println("Double: " + rs.getDouble(5));
                    System.out.println();
                }

                s.execute("DROP TABLE a;");
                System.out.println("Drop update count: " + s.getUpdateCount());
                c.close();
                System.out.println("Closed connection");
            }
            else {
                System.out.println("No connection was made");
            }
        } catch (SQLException | NullPointerException e) {
            e.printStackTrace();
        }
    }
}
