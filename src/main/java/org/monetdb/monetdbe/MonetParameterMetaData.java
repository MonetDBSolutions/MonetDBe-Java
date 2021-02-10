package org.monetdb.monetdbe;

import java.sql.ParameterMetaData;
import java.sql.SQLException;

public class MonetParameterMetaData extends MonetWrapper implements ParameterMetaData {
    protected final int parameterCount;
    /** The MonetDB types of the columns in this ResultSet as integers */
    protected final int[] types;
    /** The MonetDB types of the columns in this ResultSet as strings */
    protected final String[] monetTypes;
    /** The JDBC SQL types of the columns in this ResultSet.*/
    protected final int[] sqlTypes;
    /** The name of the Java classes corresponding to the columns in this ResultSet */
    private final String[] javaTypes;

    public MonetParameterMetaData(int parameterCount, int[] monetdbeTypes) {
        this.parameterCount = parameterCount;
        this.types = monetdbeTypes;

        this.monetTypes = new String[parameterCount];
        this.sqlTypes = new int[parameterCount];
        this.javaTypes = new String[parameterCount];

        for(int i = 0; i<parameterCount; i++ ) {
            monetTypes[i] = MonetTypes.getMonetTypeString(monetdbeTypes[i]);
            sqlTypes[i] = MonetTypes.getSQLTypeFromMonet(monetdbeTypes[i]);
            javaTypes[i] = MonetTypes.getClassForMonetType(monetdbeTypes[i]).getName();
        }
    }

    @Override
    public int getParameterCount() throws SQLException {
        return parameterCount;
    }

    //TODO Verify this
    @Override
    public int isNullable(int param) throws SQLException {
        return ParameterMetaData.parameterNullableUnknown;
    }

    @Override
    public boolean isSigned(int param) throws SQLException {
        return MonetTypes.isSigned(getParameterType(param));
    }

    //TODO SCALE
    @Override
    public int getPrecision(int param) throws SQLException {
        return 0;
    }

    //TODO SCALE
    @Override
    public int getScale(int param) throws SQLException {
        return 0;
    }

    //SQL type
    @Override
    public int getParameterType(int param) throws SQLException {
        try {
            return sqlTypes[param-1];
        } catch (IndexOutOfBoundsException e) {
            throw new SQLException("columnIndex out of bounds");
        }
    }

    //MonetDB type
    @Override
    public String getParameterTypeName(int param) throws SQLException {
        try {
            return monetTypes[param-1];
        } catch (IndexOutOfBoundsException e) {
            throw new SQLException("columnIndex out of bounds");
        }
    }

    @Override
    public String getParameterClassName(int param) throws SQLException {
        try {
            return javaTypes[param-1];
        } catch (IndexOutOfBoundsException e) {
            throw new SQLException("columnIndex out of bounds");
        }
    }

    @Override
    public int getParameterMode(int param) throws SQLException {
        return ParameterMetaData.parameterModeIn;
    }
}
