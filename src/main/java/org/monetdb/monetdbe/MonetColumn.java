package org.monetdb.monetdbe;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.nio.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.BitSet;

/**
 * Java class representation of a result MonetDB column. Stores data on one column of a {@link MonetResultSet} and allows
 * for retrieving values from the column through the getX() methods. Allows type conversion according to the JDBC standard
 * (follows table B6 of the JDBC 4.3 specification).
 */
public class MonetColumn {
    /** Stores constant length types */
    private ByteBuffer constData;
    /** Stores variable length types */
    private Object[] varData;
    /** Scale for decimal/numerical values */
    private double scale;
    /** Column name */
    private String name;
    /** MonetDBe type (int) */
    private int monetdbeType;
    /** MonetDBe type name (String), used for ResultSetMetaData */
    private String typeName;

    /** Constructor for constant length data types (called from monetdbe_result_fetch_all)
     *
     * @param name Column name
     * @param monetdbeType MonetDBe type (int)
     * @param constData Column data
     * @param scale Scale for decimal values
     */
    public MonetColumn(String name, int monetdbeType, ByteBuffer constData, double scale) {
        this.name = name;
        this.monetdbeType = monetdbeType;
        this.typeName = MonetTypes.getMonetTypeString(monetdbeType);
        this.constData = constData.order(ByteOrder.LITTLE_ENDIAN);
        this.scale = scale;
    }

    /** Constructor for variable length data types (called from monetdbe_result_fetch_all)
     *
     * @param name Column name
     * @param monetdbeType MonetDBe type (int)
     * @param varData Column data
     */
    public MonetColumn(String name, int monetdbeType, Object[] varData) {
        this.name = name;
        this.monetdbeType = monetdbeType;
        this.typeName = MonetTypes.getMonetTypeString(monetdbeType);
        this.varData = varData;
    }

    String getName() {
        return name;
    }

    int getMonetdbeType() {
        return monetdbeType;
    }

    String getTypeName() {
        return typeName;
    }

    /** Translates MonetDBe's internal scale format into Java's MathContext scale format.
     *  Example: MonetDBe scale = 1000.0, then Java scale =  3
     *  @return Scale of current column in Java's MathContext scale format
     */
    public int getScaleJDBC() {
        return ((BigDecimal.valueOf(this.scale).precision()) - 2);
    }

    /**
     * Gets the value at a specified row, as the default Java object class for the column's MonetDBe type.
     *
     * @param row Row number to get value from
     * @return Value at specified row, as the default Java object class for this column's type
     */
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

    /**
     * Gets the value at a specified row, as a Boolean object.
     * Supported types: Boolean, Byte, Short, Integer, Long, Float, Double, Decimal, String.
     * If the type if not supported, returns NULL.
     *
     * @param row Row number to get value from
     * @return If the column type is supported, value at specified row as a Boolean object. Otherwise, returns NULL
     */
    Boolean getBoolean(int row) {
        if (monetdbeType == 0) {
            return constData.get(row)!=0;
        }
        else if (monetdbeType > 0 && monetdbeType < 9) {
            return ((Number) getObject(row)).byteValue() != 0;
        }
        else if (monetdbeType == 9) {
            return Boolean.parseBoolean(getString(row));
        }
        return null;
    }

    /**
     * Gets the value at a specified row, as a Byte object.
     * Supported types: Byte, Boolean, Short, Integer, Long, Float, Double, Decimal, String.
     * If the type if not supported, returns NULL.
     *
     * @param row Row number to get value from
     * @return If the column type is supported, value at specified row as a Byte object. Otherwise, returns NULL
     */
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

    /**
     * Gets the value at a specified row, as a Short object.
     * Supported types: Short, Byte, Boolean, Integer, Long, Float, Double, Decimal, String.
     * If the type if not supported, returns NULL.
     *
     * @param row Row number to get value from
     * @return If the column type is supported, value at specified row as a Short object. Otherwise, returns NULL
     */
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

    /**
     * Gets the value at a specified row, as a Integer object.
     * Supported types: Integer, Byte, Boolean, Short, Integer, Long, Float, Double, Decimal, String.
     * If the type if not supported, returns NULL.
     *
     * @param row Row number to get value from
     * @return If the column type is supported, value at specified row as a Integer object. Otherwise, returns NULL
     */
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

    /**
     * Gets the value at a specified row, as a Long object.
     * Supported types: Long, Byte, Boolean, Short, Integer, Float, Double, Decimal, String.
     * If the type if not supported, returns NULL.
     *
     * @param row Row number to get value from
     * @return If the column type is supported, value at specified row as a Long object. Otherwise, returns NULL
     */
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

    /**
     * Gets the value at a specified row, as a Float object.
     * Supported types: Float, Byte, Boolean, Short, Integer, Long, Double, Decimal, String
     * If the type if not supported, returns NULL.
     *
     * @param row Row number to get value from
     * @return If the column type is supported, value at specified row as a Float object. Otherwise, returns NULL
     */
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

    /**
     * Gets the value at a specified row, as a Double object.
     * Supported types: Double, Byte, Boolean, Short, Integer, Long, Float, Decimal, String
     * If the type if not supported, returns NULL.
     *
     * @param row Row number to get value from
     * @return If the column type is supported, value at specified row as a Float object. Otherwise, returns NULL
     */
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

    /**
     * Gets the value at a specified row, as a BigInteger object.
     * Supported types: Decimal, Numeric
     * If the type if not supported, returns NULL.
     *
     * @param row Row number to get value from
     * @return If the column type is supported, value at specified row as a BigInteger object. Otherwise, returns NULL
     */
    BigInteger getBigInteger(int row) {
        if (monetdbeType != 5)
            return null;
        int size = MonetTypes.getMonetSize(monetdbeType);
        byte[] byteData = new byte[size];

        //Copy bytes in reverse order (BigInteger constructor takes Big-Endian byte[])
        //Using ByteBuffer's absolute get method
        for (int i = 0; i < size ; i++) {
            byteData[size-i-1] = constData.get((row*size)+i);
        }
        return new BigInteger(byteData);
    }

    /**
     * Gets the value at a specified row, as a BigDecimal object.
     * Supported types: Byte, Short, Integer, Long, Decimal
     * If the type if not supported, returns NULL.
     *
     * @param row Row number to get value from
     * @return If the column type is supported, value at specified row as a BigDecimal object. Otherwise, returns NULL
     */
    BigDecimal getBigDecimal(int row) {
        int scale = getScaleJDBC();
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

    /**
     * Gets the value at a specified row, as a String object.
     * Supported types: Byte, Boolean, Short, Integer, Long, Float, Double, Decimal, String, Blob, Date, Time, Timestamp
     *
     * @param row Row number to get value from
     * @return Value at specified row as a String object.
     */
    String getString(int row) {
        if (monetdbeType == 9) {
            return (String) varData[row];
        }
        else {
            return String.valueOf(getObject(row));
        }
    }

    /**
     * Gets the value at a specified row, as a byte[] object.
     * Supported types: Blob.
     * If the type if not supported, returns NULL.
     *
     * @param row Row number to get value from
     * @return If the column type is supported, value at specified row as a byte[] object. Otherwise, returns NULL
     */
    byte[] getBytes(int row) {
        if (monetdbeType == 10)
            return (byte[]) varData[row];
        else
            return null;
    }

    /**
     * Gets the value at a specified row, as a {@link MonetBlob} object.
     * Supported types: Blob.
     * If the type if not supported, returns NULL.
     *
     * @param row Row number to get value from
     * @return If the column type is supported, value at specified row as a {@link MonetBlob} object. Otherwise, returns NULL
     */
    MonetBlob getBlob(int row) {
        if (monetdbeType == 10)
            return new MonetBlob((byte[]) varData[row]);
        else
            return null;
    }

    /**
     * Gets the value at a specified row, as a LocalDate object.
     * Supported types: String, Date, Timestamp.
     * If the type if not supported, returns NULL.
     *
     * @param row Row number to get value from
     * @return If the column type is supported, value at specified row as a LocalDate object. Otherwise, returns NULL
     */
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
                return LocalDate.ofEpochDay(0);
        }
    }

    /**
     * Gets the value at a specified row, as a LocalTime object.
     * Supported types: String, Time, Timestamp.
     * If the type if not supported, returns NULL.
     *
     * @param row Row number to get value from
     * @return If the column type is supported, value at specified row as a LocalTime object. Otherwise, returns NULL
     */
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
                return LocalTime.ofSecondOfDay(0);
        }
    }

    /**
     * Gets the value at a specified row, as a LocalDateTime object.
     * Supported types: String, Date, Time, Timestamp.
     * If the type if not supported, returns NULL.
     *
     * @param row Row number to get value from
     * @return If the column type is supported, value at specified row as a LocalDateTime object. Otherwise, returns NULL
     */
    LocalDateTime getLocalDateTime(int row) throws DateTimeParseException {
        switch (monetdbeType) {
            case 9:
                DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss[.SSSSSS][.SSSS][.SS]");
                return  LocalDateTime.parse((String) varData[row],dtf);
            case 11:
                return LocalDateTime.ofEpochSecond(0,0,ZoneOffset.UTC).with((LocalDate) varData[row]);
            case 12:
                return LocalDateTime.ofEpochSecond(0,0,ZoneOffset.UTC).with((LocalTime) varData[row]);
            case 13:
                return (LocalDateTime) varData[row];
            default:
                return LocalDateTime.ofEpochSecond(0,0,ZoneOffset.UTC);
        }
    }
}
