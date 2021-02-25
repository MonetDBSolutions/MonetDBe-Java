package org.monetdb.monetdbe;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.nio.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
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
                return constData.asShortBuffer().get(row);
            case 3:
                return constData.asIntBuffer().get(row);
            case 4:
                return constData.asLongBuffer().get(row);
            case 5:
                return getBigInteger(row);
            case 7:
                return constData.asFloatBuffer().get(row);
            case 8:
                return constData.asDoubleBuffer().get(row);
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
            return constData.asShortBuffer().get(row);
        }
        else if (monetdbeType == 1) {
            return (short) constData.get(row);
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
            return constData.asIntBuffer().get(row);
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
            return constData.asLongBuffer().get(row);
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
            return constData.asFloatBuffer().get(row);
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
            return constData.asDoubleBuffer().get(row);
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
        //TODO Check this translation
        //int scale = -((BigDecimal.valueOf(this.scale).scale()) - 1);
        int scale = ((BigDecimal.valueOf(this.scale).precision()) - 2);
        switch (monetdbeType) {
            case 1:
                Byte unscaledByte = constData.get(row);
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
                return new BigDecimal(0);
        }
    }

    String getString(int row) {
        if (monetdbeType == 9) {
            return (String) varData[row];
        }
        else {
            return String.valueOf(getObject(row));
        }
    }

    LocalDate getLocalDate(int row) throws DateTimeParseException {
        switch (monetdbeType) {
            case 9:
                DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd");
                return  LocalDate.parse((String) varData[row],dtf);
            case 11:
                return (LocalDate) varData[row];
            case 13:
                return ((LocalDateTime) varData[row]).toLocalDate();
            default:
                return null;
        }
    }

    LocalTime getLocalTime(int row) throws DateTimeParseException {
        switch (monetdbeType) {
            case 9:
                DateTimeFormatter dtf = DateTimeFormatter.ofPattern("HH:mm:ss[.SSSSSS][.SSSS][.SS]");
                return  LocalTime.parse((String) varData[row],dtf);
            case 12:
                return (LocalTime) varData[row];
            case 13:
                return ((LocalDateTime) varData[row]).toLocalTime();
            default:
                return null;
        }
    }

    LocalDateTime getLocalDateTime(int row) throws DateTimeParseException {
        switch (monetdbeType) {
            case 9:
                DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss[.SSSSSS][.SSSS][.SS]");
                return  LocalDateTime.parse((String) varData[row],dtf);
            case 11:
                return LocalDateTime.now().with((LocalDate) varData[row]);
            case 12:
                return LocalDateTime.now().with((LocalTime) varData[row]);
            case 13:
                return (LocalDateTime) varData[row];
            default:
                return null;
        }
    }

    byte[] getBytes(int row) {
        return (byte[]) varData[row];
    }

    MonetBlob getBlob(int row) {
        return new MonetBlob((byte[]) varData[row]);
    }
}
