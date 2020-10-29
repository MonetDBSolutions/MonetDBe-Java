package nl.cwi.monetdb.monetdbe;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

public class MonetResultSetMetaData implements ResultSetMetaData {
    /** The names of the columns in this ResultSet */
    private final String[] names;
    /** The MonetDB types of the columns in this ResultSet as integers */
    private final int[] types;
    /** The MonetDB types of the columns in this ResultSet */
    private final String[] monetTypes;
    /** The JDBC SQL types of the columns in this ResultSet.*/
    private final int[] sqlTypes;

    public MonetResultSetMetaData(MonetColumn[] columns, int ncols) {
        this.names = new String[ncols];
        this.monetTypes = new String[ncols];
        this.sqlTypes = new int[ncols];
        this.types = new int[ncols];

        //TODO Review this
        for(int i = 0; i<ncols; i++ ) {
            names[i] = columns[i].getName();
            types[i] = columns[i].getMonetdbeType();
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
        return names.length;
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        //TODO
        return false;
    }

    //Pedro's code
    @Override
    public boolean isCaseSensitive(final int column) throws SQLException {
        switch (getColumnType(column)) {
            case Types.CHAR:
            case Types.LONGVARCHAR: // MonetDB doesn't use type LONGVARCHAR, it's here for completeness
            case Types.CLOB:
                return true;
            case Types.VARCHAR:
                final String monettype = getColumnTypeName(column);
                if (monettype != null && monettype.length() == 4) {
                    // data of type inet or uuid is not case sensitive
                    if ("inet".equals(monettype)
                            || "uuid".equals(monettype))
                        return false;
                }
                return true;
        }

        return false;
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        return false;
    }

    @Override
    public int isNullable(int column) throws SQLException {
        //TODO NULLABLE
        return 0;
    }

    //Pedro's Code
    @Override
    public boolean isSigned(final int column) throws SQLException {
        switch (getColumnType(column)) {
            case Types.NUMERIC:
            case Types.DECIMAL:
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.REAL:
            case Types.FLOAT:
            case Types.DOUBLE:
                return true;
            case Types.BIGINT:
                final String monettype = getColumnTypeName(column);
                if (monettype != null && monettype.length() == 3) {
                    // data of type oid or ptr is not signed
                    if ("oid".equals(monettype)
                            || "ptr".equals(monettype))
                        return false;
                }
                return true;
            case Types.BIT: // we don't use type BIT, it's here for completeness
            case Types.BOOLEAN:
            case Types.DATE:
            case Types.TIME:
            case Types.TIMESTAMP:
            default:
                return false;
        }
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        return MonetColumn.getMonetSize(getColumnType(column));
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return getColumnName(column);
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        try {
            return names[column];
        } catch (IndexOutOfBoundsException e) {
            throw new SQLException("columnIndex out of bounds");
        }
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        //TODO TABLE NAMES
        //Where do I get table and schema names in the resultset?
        return null;
    }

    @Override
    public String getTableName(int column) throws SQLException {
        //TODO TABLE NAMES
        return null;
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        //TODO SCALE
        return 0;
    }

    @Override
    public int getScale(int column) throws SQLException {
        //TODO SCALE
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
            return sqlTypes[column];
        } catch (IndexOutOfBoundsException e) {
            throw new SQLException("columnIndex out of bounds");
        }
    }

    //MonetDB type
    @Override
    public String getColumnTypeName(int column) throws SQLException {
        try {
            return monetTypes[column];
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
        return MonetColumn.getClassForType(getColumnType(column)).getName();
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
