package org.monetdb.monetdbe;

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

/**
 * A {@link ParameterMetaData} suitable for the MonetDB embedded database.
 *
 * An object that can be used to get information about the types and properties for each parameter marker in a PreparedStatement object.
 */
public class MonetParameterMetaData extends MonetWrapper implements ParameterMetaData {
    /** Number of parameters */
    protected final int parameterCount;
    /** The MonetDB types of the parameters as integers */
    protected final int[] types;
    /** The MonetDB types of the parameters as strings */
    protected final String[] monetTypes;
    /** The JDBC SQL types of the parameters */
    protected final int[] sqlTypes;
    /** The name of the Java classes corresponding to the parameters */
    private final String[] javaTypes;
    /** Digits for parameters */
    protected int[] digits;
    /** Scales for parameters */
    protected int[] scale;

    /** Constructor from types returned from the PREPARE step of a reusable query
     *
     * @param parameterCount Number of parameters in PreparedQuery
     * @param monetdbeTypes Array of types of parameters in PreparedQuery (monetdbe.h types)
     **/
    MonetParameterMetaData(int parameterCount, int[] monetdbeTypes, int[] digits, int[] scale) {
        this.parameterCount = parameterCount;
        this.types = monetdbeTypes;

        this.monetTypes = new String[parameterCount];
        this.sqlTypes = new int[parameterCount];
        this.javaTypes = new String[parameterCount];
        if (digits != null && scale != null) {
            this.digits = digits;
            this.scale = scale;
        }

        for(int i = 0; i < parameterCount; i++ ) {
            this.monetTypes[i] = MonetTypes.getMonetTypeString(monetdbeTypes[i]);
            this.sqlTypes[i] = MonetTypes.getSQLTypeFromMonet(monetdbeTypes[i]);
            this.javaTypes[i] = MonetTypes.getClassForMonetType(monetdbeTypes[i]).getName();


        }
    }

    /**
     * Retrieves the number of parameters in the PreparedStatement object for which this ParameterMetaData object contains information.
     *
     * @return the number of parameters
     */
    @Override
    public int getParameterCount() throws SQLException {
        return parameterCount;
    }

    /**
     * Retrieves whether null values are allowed in the designated parameter.
     * Currently not supported.
     *
     * @param param Parameter number (starts at 1)
     * @return the nullability status of the given parameter; one of ParameterMetaData.parameterNoNulls,
     * ParameterMetaData.parameterNullable, or ParameterMetaData.parameterNullableUnknown
     */
    @Override
    public int isNullable(int param) throws SQLException {
        return ParameterMetaData.parameterNullableUnknown;
    }

    /**
     * Retrieves whether values for the designated parameter can be signed numbers.
     *
     * @param param Parameter number (starts at 1)
     * @return true if so; false otherwise
     */
    @Override
    public boolean isSigned(int param) throws SQLException {
        return MonetTypes.isSigned(getParameterType(param));
    }

    /**
     * Retrieves the designated parameter's specified column size. The returned value represents the maximum column size for the given parameter.
     *
     * @param param Parameter number (starts at 1)
     * @return precision
     */
    @Override
    public int getPrecision(int param) throws SQLException {
        try {
            return digits[param-1];
        } catch (IndexOutOfBoundsException e) {
            throw new SQLException("columnIndex out of bounds");
        }
    }

    /**
     * Retrieves the designated parameter's number of digits to right of the decimal point. 0 is returned for data types where the scale is not applicable.
     *
     * @param param Parameter number (starts at 1)
     * @return scale
     */
    @Override
    public int getScale(int param) throws SQLException {
        try {
            return scale[param-1];
        } catch (IndexOutOfBoundsException e) {
            throw new SQLException("columnIndex out of bounds");
        }
    }

    /**
     * Retrieves the designated parameter's SQL type.
     *
     * @param param Parameter number (starts at 1)
     * @return SQL type from java.sql.Types
     * @throws SQLException if the column parameter is out of bounds
     */
    @Override
    public int getParameterType(int param) throws SQLException {
        try {
            return sqlTypes[param-1];
        } catch (IndexOutOfBoundsException e) {
            throw new SQLException("columnIndex out of bounds");
        }
    }

    /**
     * Retrieves the designated parameter's MonetDBe type name as a String.
     *
     * @param param Parameter number (starts at 1)
     * @return MonetDBe type
     * @throws SQLException if the column parameter is out of bounds
     */
    @Override
    public String getParameterTypeName(int param) throws SQLException {
        //MonetDB type
        try {
            return monetTypes[param-1];
        } catch (IndexOutOfBoundsException e) {
            throw new SQLException("columnIndex out of bounds");
        }
    }

    /**
     * Returns the fully-qualified name of the Java class whose instances are manufactured if the method
     * ResultSet.getObject is called to retrieve a value from the column.
     *
     * @param param Parameter number (starts at 1)
     * @return the fully-qualified name of the class in the Java programming language that would be used by the method
     * ResultSet.getObject to retrieve the value in the specified column
     * @throws SQLException if the column parameter is out of bounds
     */
    @Override
    public String getParameterClassName(int param) throws SQLException {
        //Java class
        try {
            return javaTypes[param-1];
        } catch (IndexOutOfBoundsException e) {
            throw new SQLException("columnIndex out of bounds");
        }
    }

    /**
     * Retrieves the designated parameter's mode. MonetDB doesn't support OUT parameters, so we return
     * ParameterMetaData.parameterModeIn always.
     *
     * @param param Parameter number (starts at 1)
     * @return ParameterMetaData.parameterModeIn
     */
    @Override
    public int getParameterMode(int param) throws SQLException {
        return ParameterMetaData.parameterModeIn;
    }
}
