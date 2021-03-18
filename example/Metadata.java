import java.sql.*;

public class Metadata {

    static void printResult(ResultSet rs, int start, int end) throws SQLException {
        while(rs.next()) {
            for (int i =start; i <= end; i++) {
                System.out.print(rs.getObject(i) + "\t");
            }
            System.out.println();
        }
        System.out.println("\n");
    }

    static void databaseMetadataGet (DatabaseMetaData dbMeta) throws SQLException {
        ResultSet rs;
        rs = dbMeta.getProcedures(null,null,null);
        System.out.println("Procedures: ");
        printResult(rs,1,8);

        rs = dbMeta.getProcedureColumns(null,null,null,null);
        System.out.println("Procedure Columns: ");
        printResult(rs,1,20);

        rs = dbMeta.getTables(null,null,null,null);
        System.out.println("Tables: ");
        printResult(rs,1,4);

        rs = dbMeta.getSchemas(null,null);
        System.out.println("Schemas: ");
        printResult(rs,1,2);

        rs = dbMeta.getColumns(null,null,null,null);
        System.out.println("Columns: ");
        printResult(rs,1,23);

        rs = dbMeta.getColumnPrivileges(null,null,null,null);
        System.out.println("Columns Privileges: ");
        printResult(rs,1,8);

        rs = dbMeta.getTablePrivileges(null,null,null);
        System.out.println("Table Privileges: ");
        printResult(rs,1,7);

        rs = dbMeta.getPrimaryKeys(null,null,null);
        System.out.println("Primary Keys: ");
        printResult(rs,1,6);

        rs = dbMeta.getImportedKeys(null,null,null);
        System.out.println("Imported Keys: ");
        printResult(rs,1,14);

        rs = dbMeta.getTypeInfo();
        System.out.println("Type info: ");
        printResult(rs,1,18);

        rs = dbMeta.getIndexInfo(null,null,null, false, false);
        System.out.println("Index info: ");
        printResult(rs,1,13);

        rs = dbMeta.getFunctions(null,null,null);
        System.out.println("Functions: ");
        printResult(rs,1,6);

        rs = dbMeta.getFunctionColumns(null,null,null,null);
        System.out.println("Function Columns: ");
        printResult(rs,1,17);
    }

    static void databaseMetadata (Connection conn) throws SQLException {
        DatabaseMetaData dbMeta = conn.getMetaData();
        ResultSet rs;

        System.out.println("Keywords: " + dbMeta.getSQLKeywords());
        System.out.println("Numeric functions: " + dbMeta.getNumericFunctions());
        System.out.println("String functions: " + dbMeta.getStringFunctions());
        System.out.println("System functions: " + dbMeta.getSystemFunctions());
        System.out.println("Timedate functions: " + dbMeta.getTimeDateFunctions());

        rs = dbMeta.getTableTypes();
        System.out.println("Table types: ");
        while(rs.next()) {
            System.out.println("\t" + rs.getObject(1));
        }


        databaseMetadataGet(dbMeta);

        rs = dbMeta.getClientInfoProperties();
        System.out.println("ClientInfoProperties: ");
        printResult(rs,1,4);

    }

    static void resultsetMetadata (Connection conn) throws SQLException {
        Statement s = conn.createStatement();
        ResultSet rs = s.executeQuery("SELECT 1, 'hey', 200000000000, 912387.3232;");
        ResultSetMetaData rsMeta = rs.getMetaData();

        int i = 1;

        while (rs.next()) {
            System.out.println("Value: " + rs.getObject(i));
            System.out.println("Name: " + rsMeta.getColumnName(i));
            System.out.println("SQL Type: " + rsMeta.getColumnType(i));
            System.out.println("Monet Type: " + rsMeta.getColumnTypeName(i));
            System.out.println("Java Type: " + rsMeta.getColumnClassName(i));
            System.out.println("Size: " + rsMeta.getColumnDisplaySize(i));
            i++;
        }
    }

    static void parameterMetadata (Connection conn) throws SQLException {
        //TODO
    }

    public static void main(String[] args) {
        Connection conn = null;
        try {
            //In-memory database
            conn = DriverManager.getConnection("jdbc:monetdb:memory:", null);

            resultsetMetadata(conn);
            databaseMetadata(conn);
            parameterMetadata(conn);
        } catch (SQLException e) {
            e.printStackTrace();
        }


    }
}
