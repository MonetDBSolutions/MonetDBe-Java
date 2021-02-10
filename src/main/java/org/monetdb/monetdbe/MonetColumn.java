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

    Buffer getConstData() {
        return constData;
    }

    Object[] getVarData() {
        return varData;
    }

    String getName() {
        return name;
    }

    int getMonetdbeType() {
        return monetdbeType;
    }

    int getSQLType() {
        return MonetTypes.getSQLTypeFromMonet(monetdbeType);
    }

    String getTypeName() {
        return typeName;
    }

    Object getObject (int row) {
        switch (monetdbeType) {
            case 0:
                return constData.get(row)!=0;
            case 1:
                return constData.get(row);
            case 2:
                return constData.getShort(row);
            case 3:
                return constData.getInt(row);
            case 4:
                return constData.getLong(row);
            case 5:
                return getBigInteger(row);
            case 7:
                return constData.getFloat(row);
            case 8:
                return constData.getDouble(row);
            case 9:
                return getString(row);
            case 10:
                return getBlob(row);
            case 11:
                return getLocalDate(row);
            case 12:
                return getLocalTime(row);
            case 13:
                return getLocalDateTime(row);
            default:
                return null;
        }
    }

    Boolean getBoolean(int row) {
        if (monetdbeType == 0) {
            return constData.get(row)!=0;
        }
        //TODO Check this conversion
        else if (monetdbeType > 0 && monetdbeType < 9) {
            return ((Number) getObject(row)).byteValue() != 0;
        }
        else if (monetdbeType == 9) {
            return Boolean.parseBoolean(getString(row));
        }
        return null;
    }

    Byte getByte(int row) {
        if (monetdbeType == 1) {
            return constData.get(row);
        }
        else if (monetdbeType == 0) {
            return getBoolean(row) ? (byte) 1 : 0;
        }
        else if (monetdbeType > 0 && monetdbeType < 9) {
            return ((Number) getObject(row)).byteValue();
        }
        else if (monetdbeType == 9) {
            return Byte.parseByte(getString(row));
        }
        return null;
    }

    Short getShort(int row) {
        if (monetdbeType == 2) {
            return constData.getShort(row);
        }
        else if (monetdbeType == 0) {
            return getBoolean(row) ? (short) 1 : 0;
        }
        else if (monetdbeType > 0 && monetdbeType < 9) {
            return ((Number) getObject(row)).shortValue();
        }
        else if (monetdbeType == 9) {
            return Short.parseShort(getString(row));
        }
        return null;
    }

    Integer getInt(int row) {
        if (monetdbeType == 3) {
            return constData.getInt(row);
        }
        else if (monetdbeType == 0) {
            return getBoolean(row) ? 1 : 0;
        }
        else if (monetdbeType > 0 && monetdbeType < 9) {
            return ((Number) getObject(row)).intValue();
        }
        else if (monetdbeType == 9) {
            return Integer.parseInt(getString(row));
        }
        return null;
    }

    Long getLong(int row) {
        if (monetdbeType == 4) {
            return constData.getLong(row);
        }
        else if (monetdbeType == 0) {
            return getBoolean(row) ? (long) 1 : 0;
        }
        else if (monetdbeType > 0 && monetdbeType < 9) {
            return ((Number) getObject(row)).longValue();
        }
        else if (monetdbeType == 9) {
            return Long.parseLong(getString(row));
        }
        return null;
    }

    Float getFloat(int row) {
        if (monetdbeType == 7) {
            return constData.getFloat(row);
        }
        else if (monetdbeType == 0) {
            return getBoolean(row) ? (float) 1 : 0;
        }
        else if (monetdbeType > 0 && monetdbeType < 9) {
            return ((Number) getObject(row)).floatValue();
        }
        else if (monetdbeType == 9) {
            return Float.parseFloat(getString(row));
        }
        return null;
    }

    Double getDouble(int row) {
        if (monetdbeType == 8) {
            return constData.getDouble(row);
        }
        else if (monetdbeType == 0) {
            return getBoolean(row) ? (double) 1 : 0;
        }
        else if (monetdbeType > 0 && monetdbeType < 9) {
            return ((Number) getObject(row)).doubleValue();
        }
        else if (monetdbeType == 9) {
            return Double.parseDouble(getString(row));
        }
        return null;
    }

    BigInteger getBigInteger(int row) {
        int size = MonetTypes.getMonetSize(monetdbeType);
        byte[] byteData = new byte[size];

        //Copy bytes in reverse order (BigInteger constructor takes Big-Endian byte[])
        //Using ByteBuffer's absolute get method
        for (int i = 0; i < size ; i++) {
            byteData[size-i-1] = constData.get((row*size)+i);
        }
        return new BigInteger(byteData);
    }

    BigDecimal getBigDecimal(int row) {
        //Translates monetdbe's internal scale format into java's MathContext scale format
        //TODO Check this translation later
        int scale = -((BigDecimal.valueOf(this.scale).scale()) - 1);

        switch (monetdbeType) {
            case 1:
                Byte unscaledByte = constData.get(row);
                return new BigDecimal(unscaledByte.intValue()).movePointRight(scale);
            case 2:
                Short unscaledShort = constData.getShort(row);
                return new BigDecimal(unscaledShort.intValue()).movePointRight(scale);
            case 3:
                Integer unscaledInt = constData.getInt(row);
                return new BigDecimal(unscaledInt).movePointLeft(scale);
            case 4:
                Long unscaledLong = constData.getLong(row);
                return new BigDecimal(unscaledLong).movePointLeft(scale);
            case 5:
                BigInteger unscaledBigInt = getBigInteger(row);
                return new BigDecimal(unscaledBigInt).movePointLeft(scale);
            default:
                return new BigDecimal(0);
        }
    }

    String getString(int row) {
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

    LocalDate getLocalDate(int row) {
        if (varData instanceof LocalDate[] && varData[row] != null)
            return (LocalDate) varData[row];
        else
            return null;
    }

    LocalTime getLocalTime(int row) {
        if (varData instanceof LocalTime[] && varData[row] != null)
            return (LocalTime) varData[row];
        else
            return null;
    }

    LocalDateTime getLocalDateTime(int row) {
        if (varData instanceof LocalDateTime[] && varData[row] != null)
            return (LocalDateTime) varData[row];
        else
            return null;
    }

    byte[] getBytes(int row) {
        return (byte[]) varData[row];
    }

    MonetBlob getBlob(int row) {
        return new MonetBlob((byte[]) varData[row]);
    }
}
