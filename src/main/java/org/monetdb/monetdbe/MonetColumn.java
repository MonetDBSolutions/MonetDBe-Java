package org.monetdb.monetdbe;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
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

    public Float getFloat(int row) {
        Float f = constData.asFloatBuffer().get(row);
        return f;
    }

    public Double getDouble(int row) {
        Double d = constData.asDoubleBuffer().get(row);
        return d;
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

    public BigDecimal getBigDecimal(int row) {
        //Translates monetdbe's internal scale format into java's MathContext scale format
        //TODO Check this translation later
        int scale = (BigDecimal.valueOf(this.scale).precision()) - 2;

        switch (monetdbeType) {
            case 1:
                Byte unscaledByte = getByte(row);
                return new BigDecimal(unscaledByte.intValue()).movePointRight(scale);
            case 2:
                Short unscaledShort = getShort(row);
                return new BigDecimal(unscaledShort.intValue()).movePointRight(scale);
            case 3:
                Integer unscaledInt = getInt(row);
                return new BigDecimal(unscaledInt).movePointLeft(scale);
            case 4:
                Long unscaledLong = getLong(row);
                return new BigDecimal(unscaledLong).movePointLeft(scale);
            case 5:
                BigInteger unscaledBigInt = getBigInteger(row);
                return new BigDecimal(unscaledBigInt).movePointLeft(scale);
            default:
                return new BigDecimal(0).movePointRight(scale);
        }
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
