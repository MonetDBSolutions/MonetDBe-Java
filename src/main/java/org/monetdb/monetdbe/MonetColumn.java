package org.monetdb.monetdbe;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.nio.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.BitSet;

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

    public short getShort(int row) {
        return constData.asShortBuffer().get(row);
    }

    public int getInt(int row) {
        switch (monetdbeType) {
            case 0:
                return getBoolean(row) ? 1 : 0;
            case 1:
                return (int) getByte(row);
            case 2:
                return (int) getShort(row);
            case 3:
                return constData.asIntBuffer().get(row);
            case 4:
                return (int) getLong(row);
            case 5:
                return getBigInteger(row).intValue();
            case 7:
                return (int) getFloat(row);
            case 8:
                return (int) getDouble(row);
            case 9:
                return Integer.parseInt(getString(row));
            default:
                return 0;
        }
    }

    public long getLong(int row) {
        return constData.getLong(row);
    }

    public float getFloat(int row) {
        return constData.getFloat(row);
    }

    public double getDouble(int row) {
        return constData.getDouble(row);
    }

    public byte getByte(int row) {
        return constData.get(row);
    }

    public BigInteger getBigInteger(int row) {
        int size = MonetTypes.getMonetSize(monetdbeType);
        byte[] byteData = new byte[size];

        //Copy bytes in reverse order (BigInteger constructor takes Big-Endian byte[])
        //Using ByteBuffer's absolute get method
        for (int i = 0; i < size ; i++) {
            byteData[size-i-1] = constData.get((row*size)+i);
        }
        return new BigInteger(byteData);
    }

    public BigDecimal getBigDecimal(int row) {
        //Translates monetdbe's internal scale format into java's MathContext scale format
        //TODO Check this translation later
        int scale = -((BigDecimal.valueOf(this.scale).scale()) - 1);

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
        if (varData != null) {
            return (String) varData[row];
        }
        else {
            //Conversion from static length types
            switch (monetdbeType) {
                case 0:
                    return String.valueOf(getBoolean(row));
                case 1:
                    return String.valueOf(getByte(row));
                case 2:
                    return String.valueOf(getShort(row));
                case 3:
                    return String.valueOf(getInt(row));
                case 4:
                    return String.valueOf(getLong(row));
                case 5:
                    return String.valueOf(getBigInteger(row));
                case 7:
                    return String.valueOf(getFloat(row));
                case 8:
                    return String.valueOf(getDouble(row));
                default:
                    return null;
            }
        }
    }

    public LocalDate getLocalDate(int row) {
        if (varData instanceof LocalDate[] && varData[row] != null)
            return (LocalDate) varData[row];
        else
            return null;
    }

    public LocalTime getLocalTime(int row) {
        if (varData instanceof LocalTime[] && varData[row] != null)
            return (LocalTime) varData[row];
        else
            return null;
    }

    public LocalDateTime getLocalDateTime(int row) {
        if (varData instanceof LocalDateTime[] && varData[row] != null)
            return (LocalDateTime) varData[row];
        else
            return null;
    }

    public byte[] getBytes(int row) {
        return (byte[]) varData[row];
    }

    public MonetBlob getBlob(int row) {
        return new MonetBlob((byte[]) varData[row]);
    }
}
