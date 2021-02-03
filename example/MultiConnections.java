import java.sql.*;

public class MultiConnections {
    public static void main(String[] args) {
        Connection conn1 = null;
        Statement s1 = null;
        try {
            //Local database
            conn1 = DriverManager.getConnection("jdbc:monetdb:/tmp/db1",null);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        if (conn1 == null) {
            System.out.println("Could not connect to db1");
            return;
        }
        System.out.println("Connected to db1");

        try {
            s1 = conn1.createStatement();
            s1.executeUpdate("CREATE TABLE db1 (i INTEGER);");
            s1.executeUpdate("INSERT INTO db1 VALUES (1),(2);");

            ResultSet rs1 = s1.executeQuery("SELECT * FROM db1;");
            System.out.println("Results from connection 1");
            while (rs1.next()) {
                System.out.println("Int: " + rs1.getInt(1));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        //Connection to same database
        Connection conn2 = null;
        Statement s2 = null;
        try {
            conn2 = DriverManager.getConnection("jdbc:monetdb:/tmp/db1",null);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        if (conn2 == null) {
            System.out.println("Could not connect to db2");
            return;
        }
        System.out.println("Connected to db1 through connection 2");

        try {
            s2 = conn2.createStatement();

            System.out.println("Results from connection 2");
            ResultSet rs2 = s2.executeQuery("SELECT * FROM db1;");
            while (rs2.next()) {
                System.out.println("Int: " + rs2.getInt(1));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        //Connection 1 can still be used
        try {
            ResultSet rs1Sum = s1.executeQuery("SELECT sum(i) FROM db1;");
            System.out.println("Results from connection 1 (again)");
            while (rs1Sum.next()) {
                System.out.println("Sum: " + rs1Sum.getInt(1));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }


        //Connecting to another database
        Connection conn3 = null;
        try {
            conn3 = DriverManager.getConnection("jdbc:monetdb:/tmp/db2",null);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        if (conn3 == null) {
            System.out.println("Could not connect to db2 through connection 3");
            return;
        }
        System.out.println("Connected to db2 through connection 3");

        try {
            Statement s3 = conn3.createStatement();
            s3.executeUpdate("CREATE TABLE db2 (i INTEGER);");
            s3.executeUpdate("INSERT INTO db2 VALUES (30),(40);");

            ResultSet rs3 = s3.executeQuery("SELECT * FROM db2;");
            System.out.println("Results from connection 3");
            while (rs3.next()) {
                System.out.println("Int: " + rs3.getInt(1));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        //Connection 1 can still be used
        try {
            ResultSet rs1sum = s1.executeQuery("SELECT sum(i) FROM db1;");
            System.out.println("Results from connection 1 (once more)");
            while (rs1sum.next()) {
                System.out.println("Sum: " + rs1sum.getInt(1));
            }

            s1.execute("DROP TABLE db1;");
            s2.execute("DROP TABLE db1;");

            System.out.println("Closing all connections");
            conn1.close();
            conn2.close();
            conn3.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }


    }
}
