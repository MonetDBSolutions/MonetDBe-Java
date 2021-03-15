package org.monetdb.monetdbe;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

/**
 * A {@link ResultSetMetaData} suitable for the MonetDB embedded database.
 *
 * An object that can be used to get information about the types and properties of the columns in a ResultSet object.
 */
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

    private final int[] scales;

    //Constructor for PreparedStatement without query execution
    MonetResultSetMetaData(String[] resultNames, int[] resultMonetTypes, int ncols) {
        this.names = resultNames;
        this.types = resultMonetTypes;

        this.monetTypes = new String[ncols];
        this.monetTypesInt = new int[ncols];
        this.sqlTypes = new int[ncols];
        this.javaTypes = new String[ncols];
        this.scales = new int[ncols];

        for(int i = 0; i<ncols; i++ ) {
            monetTypes[i] = MonetTypes.getMonetTypeString(resultMonetTypes[i]);
            monetTypesInt[i] = resultMonetTypes[i];
            sqlTypes[i] = MonetTypes.getSQLTypeFromMonet(resultMonetTypes[i]);
            javaTypes[i] = MonetTypes.getClassForMonetType(resultMonetTypes[i]).getName();
        }
    }

    /** Constructor from a ResultSet returned from a query */
    MonetResultSetMetaData(MonetColumn[] columns, int ncols) {
        this.names = new String[ncols];
        this.types = new int[ncols];
        this.monetTypes = new String[ncols];
        this.monetTypesInt = new int[ncols];
        this.sqlTypes = new int[ncols];
        this.javaTypes = new String[ncols];
        this.scales = new int[ncols];

        for(int i = 0; i<ncols; i++ ) {
            names[i] = columns[i].getName();
            types[i] = columns[i].getMonetdbeType();
            monetTypes[i] = columns[i].getTypeName();
            monetTypesInt[i] = columns[i].getMonetdbeType();
            sqlTypes[i] = MonetTypes.getSQLTypeFromMonet(columns[i].getMonetdbeType());
            javaTypes[i] = MonetTypes.getClassForMonetType(columns[i].getMonetdbeType()).getName();
            scales[i] = columns[i].getScaleJDBC();
        }
    }

    /**
     * Returns the column names of the ResultSet. Used for {@link MonetResultSet#findColumn(String) findColumn(String)}.
     *
     * @return Array of Strings with column names.
     */
    protected String[] getNames() {
        return names;
    }

    /**
     * Returns the number of columns in this ResultSet object.
     *
     * @return The number of columns.
     */
    @Override
    public int getColumnCount() throws SQLException {
        return names.length;
    }

    /**
     * Indicates whether the designated column is automatically numbered.
     * Currently not supported.
     *
     * @param column Column number (starts at 1)
     * @return true if so; false otherwise
     */
    //TODO Not possible to check right now, not available in C API (Not in monetdbe_column)
    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        return false;
    }

    /**
     * Indicates whether a column's case matters.
     *
     * @param column Column number (starts at 1)
     * @return true for all character string columns, else false
     */
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

    /**
     * Indicates whether the designated column can be used in a
     * where clause.
     *
     * Because all columns can be used in a where clause, we return always true.
     *
     * @param column Column number (starts at 1)
     * @return true
     */
    @Override
    public boolean isSearchable(int column) throws SQLException {
        return true;
    }

    /**
     * Indicates whether the designated column is a cash value.
     * From the MonetDB database perspective it is by definition
     * unknown whether the value is a currency, so we return false here.
     *
     * @param column Column number (starts at 1)
     * @return false
     */
    @Override
    public boolean isCurrency(int column) throws SQLException {
        return false;
    }

    /**
     * Indicates the nullability of values in the designated column.
     * Currently not supported.
     *
     * @param column Column number (starts at 1)
     * @return true if so; false otherwise
     */
    //TODO Not possible to check right now, not available in C API (Not in monetdbe_column)
    @Override
    public int isNullable(int column) throws SQLException {
        return ResultSetMetaData.columnNullableUnknown;
    }

    /**
     * Indicates whether values in the designated column are signed
     * numbers.
     * Within MonetDB all numeric types are signed.
     *
     * @param column Column number (starts at 1)
     * @return true if so; false otherwise
     */
    @Override
    public boolean isSigned(int column) throws SQLException {
        return MonetTypes.isSigned(getColumnType(column));
    }

    /**
     * Indicates the designated column's normal maximum width in
     * characters.
     * Currently not supported.
     *
     * @param column Column number (starts at 1)
     * @return the normal maximum number of characters allowed as the
     *         width of the designated column
     */
    //TODO Not possible to check right now, not available in C API (Not in monetdbe_column)
    //Similar to getPrecision
    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        return 0;
    }

    /**
     * Gets the designated column's suggested title for use in printouts and displays.
     *
     * @param column Column number (starts at 1)
     * @return the suggested column title
     * @throws SQLException if the column parameter is out of bounds
     */
    @Override
    public String getColumnLabel(int column) throws SQLException {
        return getColumnName(column);
    }

    /**
     * Get the designated column's name.
     *
     * @param column Column number (starts at 1)
     * @return column name
     * @throws SQLException if the column parameter is out of bounds
     */
    @Override
    public String getColumnName(int column) throws SQLException {
        try {
            return names[column-1];
        } catch (IndexOutOfBoundsException e) {
            throw new SQLException("columnIndex out of bounds");
        }
    }

    /**
     * Get the designated column's table's schema.
     *
     * @param column Column number (starts at 1)
     * @return schema name or "" if not applicable
     */
    //TODO Not possible to check right now, not available in C API (Not in monetdbe_column)
    @Override
    public String getSchemaName(int column) throws SQLException {
        return "";
    }

    /**
     * Gets the designated column's table name.
     *
     * @param column Column number (starts at 1)
     * @return table name or "" if not applicable
     */
    //TODO Not possible to check right now, not available in C API (Not in monetdbe_column)
    @Override
    public String getTableName(int column) throws SQLException {
        return "";
    }

    /**
     * Get the designated column's specified column size. The returned value represents the maximum column size for the designated column.
     *
     * @param column Column number (starts at 1)
     * @return precision
     */
    //TODO Not possible to check right now, not available in C API (Not in monetdbe_column)
    @Override
    public int getPrecision(int column) throws SQLException {
        return MonetTypes.getPrecision(getColumnType(column));
    }

    /**
     * Gets the designated column's number of digits to right of the decimal point. 0 is returned for data types where the scale is not applicable.
     *
     * @param column Column number (starts at 1)
     * @return scale
     */
    @Override
    public int getScale(int column) throws SQLException {
        return scales[column-1];
    }

    /**
     * Gets the designated column's table's catalog name.
     * MonetDB does not support the catalog naming concept, so we return "".
     *
     * @param column Column number (starts at 1)
     * @return ""
     */
    @Override
    public String getCatalogName(int column) throws SQLException {
        return "";
    }

    /**
     * Retrieves the designated column's SQL type.
     *
     * @param column Column number (starts at 1)
     * @return SQL type from java.sql.Types
     * @throws SQLException if the column parameter is out of bounds
     */
    @Override
    public int getColumnType(int column) throws SQLException {
        //SQL type
        try {
            return sqlTypes[column-1];
        } catch (IndexOutOfBoundsException e) {
            throw new SQLException("columnIndex out of bounds");
        }
    }

    /**
     * Retrieves the designated column's MonetDBe type name as a String.
     *
     * @param column Column number (starts at 1)
     * @return MonetDBe type
     * @throws SQLException if the column parameter is out of bounds
     */
    @Override
    public String getColumnTypeName(int column) throws SQLException {
        //MonetDB type
        try {
            return monetTypes[column-1];
        } catch (IndexOutOfBoundsException e) {
            throw new SQLException("columnIndex out of bounds");
        }
    }

    /**
     * Retrieves the designated column's MonetDBe type name as an integer.
     *
     * @param column Column number (starts at 1)
     * @return MonetDBe type as an integer
     * @throws SQLException if the column parameter is out of bounds
     */
    public int getColumnTypeInt(int column) throws SQLException {
        //MonetDB type
        try {
            return monetTypesInt[column-1];
        } catch (IndexOutOfBoundsException e) {
            throw new SQLException("columnIndex out of bounds");
        }
    }

    /**
     * Returns the fully-qualified name of the Java class whose instances are manufactured if the method
     * ResultSet.getObject is called to retrieve a value from the column.
     *
     * @param column Column number (starts at 1)
     * @return the fully-qualified name of the class in the Java programming language that would be used by the method
     * ResultSet.getObject to retrieve the value in the specified column
     * @throws SQLException if the column parameter is out of bounds
     */
    @Override
    public String getColumnClassName(int column) throws SQLException {
        //Java class
        try {
            return javaTypes[column-1];
        } catch (IndexOutOfBoundsException e) {
            throw new SQLException("columnIndex out of bounds");
        }
    }

    /**
     * Indicates whether the designated column is definitely not
     * writable. MonetDB does not support cursor updates, so
     * nothing is writable and we always return true.
     *
     * @param column Column number (starts at 1)
     * @return true
     */
    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return true;
    }

    /**
     * Indicates whether it is possible for a write on the
     * designated column to succeed. MonetDB does not support cursor updates, so
     * nothing is writable and we always return false.
     *
     * @param column Column number (starts at 1)
     * @return false;
     */
    @Override
    public boolean isWritable(int column) throws SQLException {
        return false;
    }

    /**
     * Indicates whether a write on the designated column will
     * definitely succeed. MonetDB does not support cursor updates, so
     * nothing is writable and we always return false.
     *
     * @param column Column number (starts at 1)
     * @return false;
     */
    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return false;
    }
}
