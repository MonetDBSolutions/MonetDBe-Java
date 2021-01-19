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
        this.typeName = MonetTypes.getMonetTypeString(monetdbeType);
    }

    public MonetColumn(String name, int monetdbeType, ByteBuffer constData) {
        this.name = name;
        this.monetdbeType = monetdbeType;
        this.typeName = MonetTypes.getMonetTypeString(monetdbeType);
        this.constData = constData.order(ByteOrder.LITTLE_ENDIAN);
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

    public String getTypeName() {
        return typeName;
    }

    public boolean getBoolean(int row) {
        return ((ByteBuffer) constData).get(row)!=0;
    }

    public Short getShort(int row) {
        return ((ByteBuffer) constData).asShortBuffer().get(row);
    }

    public Integer getInt(int row) {
        return ((ByteBuffer) constData).asIntBuffer().get(row);
    }

    public Long getLong(int row) {
        return ((ByteBuffer) constData).asLongBuffer().get(row);
    }

    public Integer getSize(int row) {
        return getInt(row);
    }

    public Float getFloat(int row) {
        return ((ByteBuffer) constData).asFloatBuffer().get(row);
    }

    public Double getDouble(int row) {
        return ((ByteBuffer) constData).asDoubleBuffer().get(row);
    }

    public byte getByte(int row) {
        return ((ByteBuffer) constData).get(row);
    }

    //TODO Test
    public BigInteger getBigInteger(int row) {
        byte[] byteData = new byte[16];
        ((ByteBuffer) constData).get(byteData,row*16,16);
        return new BigInteger(byteData);
    }

    //TODO Test
    public BigDecimal getBigDecimal(int row) {
        byte[] byteData = new byte[16];
        ((ByteBuffer) constData).get(byteData,row*16,16);
        return new BigDecimal(new BigInteger(byteData));
    }

    public String getString(int row) {
        return (String) varData[row];
    }

    public byte[] getBlob(int row) {
        return (byte[]) varData[row];
    }
}
