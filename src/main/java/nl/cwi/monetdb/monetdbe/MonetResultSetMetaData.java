package nl.cwi.monetdb.monetdbe;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class MonetResultSetMetaData implements ResultSetMetaData {
    /** The names of the columns in this ResultSet */
    private final String[] names;
    /** The MonetDB types of the columns in this ResultSet */
    private final String[] monetTypes;
    /** The JDBC SQL types of the columns in this ResultSet.*/
    private final int[] sqlTypes;
    /** The number of columns in this ResultSet */
    private final int columnCount;

    public MonetResultSetMetaData(MonetColumn[] columns, int ncols) {
        this.names = new String[ncols];
        this.monetTypes = new String[ncols];
        this.sqlTypes = new int[ncols];
        this.columnCount = ncols;

        for(int i = 0; i<ncols; i++ ) {
            names[i] = columns[i].getName();
            monetTypes[i] = columns[i].getTypeName();
            sqlTypes[i] = MonetColumn.getSQLType(columns[i].getTypeName());
        }
    }

    public String[] getNames() {
        return names;
    }

    public String[] getMonetTypes() {
        return monetTypes;
    }

    public int[] getSqlTypes() {
        return sqlTypes;
    }

    @Override
    public int getColumnCount() throws SQLException {
        return columnCount;
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        //TODO
        return false;
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        return false;
    }

    @Override
    public int isNullable(int column) throws SQLException {
        //TODO
        return 0;
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        //TODO
        return false;
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        String type = monetTypes[column];
        if(type.equals("monetdbe_str") || type.equals("monetdbe_blob")) {
            //TODO What to return here?
            return 255;
        }
        else {
            return MonetColumn.getMonetSize(type);
        }
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return null;
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        return null;
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return null;
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        return 0;
    }

    @Override
    public int getScale(int column) throws SQLException {
        return 0;
    }

    @Override
    public String getTableName(int column) throws SQLException {
        return null;
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return null;
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        return 0;
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        return null;
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        return null;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }
}
