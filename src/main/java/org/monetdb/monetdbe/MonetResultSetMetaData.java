package org.monetdb.monetdbe;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

public class MonetResultSetMetaData extends MonetWrapper implements ResultSetMetaData {
    /** The names of the columns in this ResultSet */
    private final String[] names;
    /** The MonetDB types of the columns in this ResultSet as integers */
    private final int[] types;
    /** The MonetDB types of the columns in this ResultSet as strings */
    private final String[] monetTypes;
    /** The MonetDB types of the columns in this ResultSet as ints */
    private final int[] monetTypesInt;
    /** The JDBC SQL types of the columns in this ResultSet */
    private final int[] sqlTypes;
    /** The name of the Java classes corresponding to the columns in this ResultSet */
    private final String[] javaTypes;

    //Constructor for PreparedStatement without query execution
    public MonetResultSetMetaData(String[] resultNames, int[] resultMonetTypes, int ncols) {
        this.names = resultNames;
        this.types = resultMonetTypes;

        this.monetTypes = new String[ncols];
        this.monetTypesInt = new int[ncols];
        this.sqlTypes = new int[ncols];
        this.javaTypes = new String[ncols];

        for(int i = 0; i<ncols; i++ ) {
            monetTypes[i] = MonetTypes.getMonetTypeString(resultMonetTypes[i]);
            monetTypesInt[i] = resultMonetTypes[i];
            sqlTypes[i] = MonetTypes.getSQLTypeFromMonet(resultMonetTypes[i]);
            javaTypes[i] = MonetTypes.getClassForMonetType(resultMonetTypes[i]).getName();
        }
    }

    //Constructor for ResultSet returned from a query
    public MonetResultSetMetaData(MonetColumn[] columns, int ncols) {
        this.names = new String[ncols];
        this.types = new int[ncols];
        this.monetTypes = new String[ncols];
        this.monetTypesInt = new int[ncols];
        this.sqlTypes = new int[ncols];
        this.javaTypes = new String[ncols];

        for(int i = 0; i<ncols; i++ ) {
            names[i] = columns[i].getName();
            types[i] = columns[i].getMonetdbeType();
            monetTypes[i] = columns[i].getTypeName();
            monetTypesInt[i] = columns[i].getMonetdbeType();
            sqlTypes[i] = MonetTypes.getSQLTypeFromMonet(columns[i].getMonetdbeType());
            javaTypes[i] = MonetTypes.getClassForMonetType(columns[i].getMonetdbeType()).getName();
        }
    }

    public String[] getNames() {
        return names;
    }

    @Override
    public int getColumnCount() throws SQLException {
        return names.length;
    }

    //TODO Verify. Should we call getColumns to check if it is an auto-increment numerical column?
    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isCaseSensitive(final int column) throws SQLException {
        switch (getColumnType(column)) {
            case Types.CHAR:
            case Types.LONGVARCHAR:
            case Types.CLOB:
            case Types.VARCHAR:
                return true;
            default:
                return false;
        }
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        return false;
    }

    //TODO Verify this
    @Override
    public int isNullable(int column) throws SQLException {
        return ResultSetMetaData.columnNullableUnknown;
    }

    @Override
    public boolean isSigned(final int column) throws SQLException {
        return MonetTypes.isSigned(getColumnType(column));
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        return MonetTypes.getMonetSize(getColumnTypeInt(column)) * 8;
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return getColumnName(column);
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        try {
            return names[column-1];
        } catch (IndexOutOfBoundsException e) {
            throw new SQLException("columnIndex out of bounds");
        }
    }

    //TODO TABLE NAMES
    @Override
    public String getSchemaName(int column) throws SQLException {
        //Where do I get table and schema names in the resultset?
        return null;
    }

    //TODO TABLE NAMES
    @Override
    public String getTableName(int column) throws SQLException {
        return null;
    }

    //TODO SCALE
    @Override
    public int getPrecision(int column) throws SQLException {
        return 0;
    }

    //TODO SCALE
    @Override
    public int getScale(int column) throws SQLException {
        return 0;
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return null;	// MonetDB does NOT support catalogs
    }

    //SQL type
    @Override
    public int getColumnType(int column) throws SQLException {
        try {
            return sqlTypes[column-1];
        } catch (IndexOutOfBoundsException e) {
            throw new SQLException("columnIndex out of bounds");
        }
    }

    //MonetDB type
    @Override
    public String getColumnTypeName(int column) throws SQLException {
        try {
            return monetTypes[column-1];
        } catch (IndexOutOfBoundsException e) {
            throw new SQLException("columnIndex out of bounds");
        }
    }

    //MonetDB type
    public int getColumnTypeInt(int column) throws SQLException {
        try {
            return monetTypesInt[column-1];
        } catch (IndexOutOfBoundsException e) {
            throw new SQLException("columnIndex out of bounds");
        }
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return true;
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
        try {
            return javaTypes[column-1];
        } catch (IndexOutOfBoundsException e) {
            throw new SQLException("columnIndex out of bounds");
        }
    }
}
