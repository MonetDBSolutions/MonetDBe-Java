package nl.cwi.monetdb.monetdbe;

import java.sql.ParameterMetaData;
import java.sql.SQLException;

public class MonetParameterMetaData extends MonetWrapper implements ParameterMetaData {
    protected int parameterCount;


    public MonetParameterMetaData() {
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
        return false;
    }

    @Override
    public int getPrecision(int param) throws SQLException {
        return 0;
    }

    @Override
    public int getScale(int param) throws SQLException {
        return 0;
    }

    @Override
    public int getParameterType(int param) throws SQLException {
        return 0;
    }

    @Override
    public String getParameterTypeName(int param) throws SQLException {
        return null;
    }

    @Override
    public String getParameterClassName(int param) throws SQLException {
        return null;
    }

    @Override
    public int getParameterMode(int param) throws SQLException {
        return ParameterMetaData.parameterModeIn;
    }
}
