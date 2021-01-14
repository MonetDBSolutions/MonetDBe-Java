package nl.cwi.monetdb.monetdbe;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.*;
import java.sql.*;
import java.util.Map;

public class MonetColumn {
    private Buffer constData;
    private Object[] varData;
    private String name;
    private int monetdbeType;
    private String typeName;


    public MonetColumn(String name, int monetdbeType) {
        this.name = name;
        this.monetdbeType = monetdbeType;
        this.typeName = MonetTypes.monetdbeTypes[monetdbeType];
    }

    public MonetColumn(String name, int monetdbeType, ByteBuffer constData) {
        this.name = name;
        this.monetdbeType = monetdbeType;
        this.typeName = MonetTypes.monetdbeTypes[monetdbeType];
        this.constData = constData.order(ByteOrder.LITTLE_ENDIAN);
    }

    public MonetColumn(String name, int monetdbeType, Object[] varData) {
        this.name = name;
        this.monetdbeType = monetdbeType;
        this.typeName = MonetTypes.monetdbeTypes[monetdbeType];
        this.varData = varData;
    }

    public Buffer getConstData() {
        return constData;
    }

    public Object[] getVarData() {
        return varData;
    }

    public String getName() {
        return name;
    }

    public int getMonetdbeType() {
        return monetdbeType;
    }

    public String getTypeName() {
        return typeName;
    }

    //Constant length types
    public boolean getBoolean(int row) throws SQLException {
        if (monetdbeType == 0)  {
            return ((ByteBuffer) constData).get(row)!=0;
        }
        else {
            throw new SQLException("Column is not bool value");
        }
    }

    public Short getShort(int row) throws SQLException {
        if (monetdbeType == 1 || monetdbeType == 2)  {
            return ((ByteBuffer) constData).asShortBuffer().get(row);
        }
        else {
            throw new SQLException("Column is not short value");
        }
    }

    public Integer getInt(int row) throws SQLException {
        if (monetdbeType ==3 || monetdbeType == 6)  {
            return ((ByteBuffer) constData).asIntBuffer().get(row);
        }
        else {
            throw new SQLException("Column is not int value");
        }
    }

    public Long getLong(int row) throws SQLException {
        if (monetdbeType == 4)  {
            return ((ByteBuffer) constData).asLongBuffer().get(row);
        }
        else {
            throw new SQLException("Column is not long value (type " + monetdbeType + ")");
        }
    }

    public Integer getSize(int row) throws  SQLException {
        return getInt(row);
    }

    public Float getFloat(int row) throws SQLException {
        if (monetdbeType == 7)  {
            return ((ByteBuffer) constData).asFloatBuffer().get(row);
        }
        else {
            throw new SQLException("Column is not float value");
        }
    }

    public Double getDouble(int row) throws SQLException {
        if (monetdbeType == 8)  {
            return ((ByteBuffer) constData).asDoubleBuffer().get(row);
        }
        else {
            throw new SQLException("Column is not double value");
        }
    }

    //TODO Test
    public BigInteger getBigInteger(int row) throws SQLException {
        if (monetdbeType == 5)  {
            byte[] byteData = new byte[16];
            ((ByteBuffer) constData).get(byteData,row*16,16);
            return new BigInteger(byteData);
        }
        else {
            throw new SQLException("Column is not BigInteger value");
        }
    }

    //TODO Test
    public BigDecimal getBigDecimal(int row) throws SQLException {
        if (monetdbeType == 5)  {
            byte[] byteData = new byte[16];
            ((ByteBuffer) constData).get(byteData,row*16,16);
            return new BigDecimal(new BigInteger(byteData));
        }
        else {
            throw new SQLException("Column is not BigDecimal value");
        }
    }

    //Variable length types
    public String getString(int row) throws SQLException {
        if(monetdbeType > 9) {
            return (String) varData[row];
        }
        else {
            throw new SQLException("Column is not string or date value");
        }
    }

    public byte getByte(int row) throws  SQLException {
        if(monetdbeType < 9) {
            return ((ByteBuffer) constData).get(row);
        }
        else {
            return 0;
        }
    }
}
