package org.monetdb.monetdbe;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.*;

public class MonetColumn {
    private ByteBuffer constData;
    private double scale;
    private Object[] varData;
    private String name;
    private int monetdbeType;
    private String typeName;

    public MonetColumn(String name, int monetdbeType) {
        this.name = name;
        this.monetdbeType = monetdbeType;
        this.typeName = MonetTypes.getMonetTypeString(monetdbeType);
    }

    public MonetColumn(String name, int monetdbeType, ByteBuffer constData, double scale) {
        this.name = name;
        this.monetdbeType = monetdbeType;
        this.typeName = MonetTypes.getMonetTypeString(monetdbeType);
        this.constData = constData.order(ByteOrder.LITTLE_ENDIAN);
        this.scale = scale;
    }

    public MonetColumn(String name, int monetdbeType, Object[] varData) {
        this.name = name;
        this.monetdbeType = monetdbeType;
        this.typeName = MonetTypes.getMonetTypeString(monetdbeType);
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

    public int getSQLType() {
        return MonetTypes.getSQLTypeFromMonet(monetdbeType);
    }

    public String getTypeName() {
        return typeName;
    }

    public boolean getBoolean(int row) {
        return constData.get(row)!=0;
    }

    public Short getShort(int row) {
        return constData.asShortBuffer().get(row);
    }

    public Integer getInt(int row) {
        return constData.asIntBuffer().get(row);
    }

    public Long getLong(int row) {
        return constData.asLongBuffer().get(row);
    }

    public Integer getSize(int row) {
        return getInt(row);
    }

    public Float getFloat(int row) {
        Float f = constData.asFloatBuffer().get(row);
        return f.isNaN() ? 0 : f;
    }

    public Double getDouble(int row) {
        Double d = constData.asDoubleBuffer().get(row);
        return d.isNaN() ? 0 : d;
    }

    public byte getByte(int row) {
        return constData.get(row);
    }

    //TODO Bytes aren't being interpreted correctly
    public BigInteger getBigInteger(int row) {
        int size = MonetTypes.getMonetSize(monetdbeType);
        byte[] byteData = new byte[size];
        constData.get(byteData,0,size);
        System.out.println("BigInt type: " + monetdbeType + " / Size: " + size + " / Approximate value: " + new BigInteger(byteData).longValue());
        return new BigInteger(byteData);
    }

    //TODO Isn't working correctly (because of BigInteger function?)
    //TODO Should this function test the size of the unscaled value and use the appropriate number type (i.e. not using BigInteger if it fits into an int)
    public BigDecimal getBigDecimal(int row) {
        return new BigDecimal(getBigInteger(row),(int) scale);
    }

    public String getString(int row) {
        return (String) varData[row];
    }

    public byte[] getBytes(int row) {
        return (byte[]) varData[row];
    }

    public MonetBlob getBlob(int row) {
        return new MonetBlob((byte[]) varData[row]);
    }
}
