package nl.cwi.monetdb.monetdbe;

import java.math.BigDecimal;
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
        this.typeName = monetdbeTypes[monetdbeType];
    }

    public MonetColumn(String name, int monetdbeType, ByteBuffer constData) {
        this.name = name;
        this.monetdbeType = monetdbeType;
        this.typeName = monetdbeTypes[monetdbeType];
        this.constData = constData.order(ByteOrder.LITTLE_ENDIAN);
    }

    public MonetColumn(String name, int monetdbeType, Object[] varData) {
        this.name = name;
        this.monetdbeType = monetdbeType;
        this.typeName = monetdbeTypes[monetdbeType];
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
        if (monetdbeType ==4)  {
            return ((ByteBuffer) constData).asLongBuffer().get(row);
        }
        else {
            throw new SQLException("Column is not long value");
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

    //Variable length types
    public String getString(int row) throws SQLException {
        if(monetdbeType == 9 || monetdbeType == 11 || monetdbeType == 12 || monetdbeType == 13) {
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

    //TYPE MAPPINGS
    //TODO Should this be here?
    private static final String[] monetdbeTypes = {"monetdbe_bool","monetdbe_int8_t","monetdbe_int16_t","monetdbe_int32_t","monetdbe_int64_t","monetdbe_int128_t","monetdbe_size_t","monetdbe_float","monetdbe_double","monetdbe_str","monetdbe_blob","monetdbe_date","monetdbe_time","monetdbe_timestamp","monetdbe_type_unknown"};
    private static final String[] sqlTypes = {"CHAR","VARCHAR","LONGVARCHAR","NUMERIC","DECIMAL","BOOLEAN","BIT","TINYINT","SMALLINT","INTEGER","BIGINT","REAL","FLOAT","DOUBLE","BINARY","VARBINARY","LONGVARBINARY","DATE","TIME","TIMESTAMP","CLOB","BLOB"};
    private static final Class[] javaTypes = {String.class,BigDecimal.class,Boolean.class,Short.class,Integer.class,Long.class,Float.class,Double.class,byte[].class,java.sql.Date.class,Time.class,Timestamp.class,Clob.class,Blob.class};

    /** A static Map containing the mapping between MonetDB types and Java SQL types */
    private static final java.util.Map<String, Integer> typeMapMonetdbe = new java.util.HashMap<String, Integer>();
    static {
        typeMapMonetdbe.put("monetdbe_bool", Integer.valueOf(Types.BOOLEAN));
        typeMapMonetdbe.put("monetdbe_int8_t", Integer.valueOf(Types.TINYINT));
        typeMapMonetdbe.put("monetdbe_int16_t", Integer.valueOf(Types.SMALLINT));
        typeMapMonetdbe.put("monetdbe_int32_t", Integer.valueOf(Types.INTEGER));
        typeMapMonetdbe.put("monetdbe_int64_t", Integer.valueOf(Types.BIGINT));
        typeMapMonetdbe.put("monetdbe_size_t", Integer.valueOf(Types.INTEGER));
        typeMapMonetdbe.put("monetdbe_float", Integer.valueOf(Types.REAL));
        typeMapMonetdbe.put("monetdbe_double", Integer.valueOf(Types.DOUBLE));
        typeMapMonetdbe.put("monetdbe_str", Integer.valueOf(Types.VARCHAR));
        typeMapMonetdbe.put("monetdbe_blob", Integer.valueOf(Types.BLOB));
        typeMapMonetdbe.put("monetdbe_date", Integer.valueOf(Types.DATE));
        typeMapMonetdbe.put("monetdbe_time", Integer.valueOf(Types.TIME));
        typeMapMonetdbe.put("monetdbe_timestamp", Integer.valueOf(Types.TIMESTAMP));
        //TODO
        typeMapMonetdbe.put("monetdbe_int128_t", 0);
        typeMapMonetdbe.put("monetdbe_unknown", 0);
    }

    /** A static Map containing the mapping between MonetDB types and Java SQL types */
    private static final java.util.Map<String, Integer> sizeMapMonetdbe = new java.util.HashMap<String, Integer>();
    static {
        sizeMapMonetdbe.put("monetdbe_bool", 8);
        sizeMapMonetdbe.put("monetdbe_int8_t", 8);
        sizeMapMonetdbe.put("monetdbe_int16_t", 16);
        sizeMapMonetdbe.put("monetdbe_int32_t", 32);
        sizeMapMonetdbe.put("monetdbe_int64_t", 64);
        sizeMapMonetdbe.put("monetdbe_int128_t", 128);
        sizeMapMonetdbe.put("monetdbe_size_t", 32);
        sizeMapMonetdbe.put("monetdbe_float", 32);
        sizeMapMonetdbe.put("monetdbe_double", 64);
        sizeMapMonetdbe.put("monetdbe_str", 0);
        sizeMapMonetdbe.put("monetdbe_blob", 0);
        sizeMapMonetdbe.put("monetdbe_date", 10);
        sizeMapMonetdbe.put("monetdbe_time", 8);
        sizeMapMonetdbe.put("monetdbe_timestamp", 19);
        typeMapMonetdbe.put("monetdbe_unknown", 0);
    }

    final static int getMonetSize(final String monetdbetype) {
        return sizeMapMonetdbe.get(monetdbetype);
    }

    //TODO Fix this (Wrong for str and blobs)
    final static int getMonetSize(final int monetdbetype) {
        return sizeMapMonetdbe.get(monetdbeTypes[monetdbetype]);
    }

    final static int getSQLType(final String monetdbetype) {
        return typeMapMonetdbe.get(monetdbetype);
    }

    final static int getMonetTypeFromTypeString (final String monetdbetype) {
        for (int i = 0; i < monetdbeTypes.length; i++) {
            if (monetdbeTypes[i].equals(monetdbetype)) {
                return i;
            }
        }
        return -1;
    }

    final static String getMonetTypeString(final int sqltype) {
        for (Map.Entry<String, Integer> entry : typeMapMonetdbe.entrySet()) {
            if (entry.getValue() == sqltype) {
                return entry.getKey();
            }
        }
        return "";
    }

    final static int getMonetTypeInt(final int sqltype) {
        for (Map.Entry<String, Integer> entry : typeMapMonetdbe.entrySet()) {
            if (entry.getValue() == sqltype) {
                return getMonetTypeFromTypeString(entry.getKey());
            }
        }
        return -1;
    }

    final static int getSQLType(final int monetdbetype) {
        return typeMapMonetdbe.get(monetdbeTypes[monetdbetype]);
    }

    final static Class<?> getClassForMonetType(final int monetdbeType) { return getClassForType(getSQLType(monetdbeType));}

    //Pedro's Code
    final static Class<?> getClassForType(final int type) {
        switch(type) {
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                return String.class;
            case Types.NUMERIC:
            case Types.DECIMAL:
                return BigDecimal.class;
            case Types.BOOLEAN:
                return Boolean.class;
            case Types.BIT: // MonetDB doesn't support type BIT, it's here for completeness
            case Types.TINYINT:
            case Types.SMALLINT:
                return Short.class;
            case Types.INTEGER:
                return Integer.class;
            case Types.BIGINT:
                return Long.class;
            case Types.REAL:
                return Float.class;
            case Types.FLOAT:
            case Types.DOUBLE:
                return Double.class;
            case Types.BINARY:      // MonetDB currently does not support these
            case Types.VARBINARY:   // see treat_blob_as_binary property
            case Types.LONGVARBINARY:
                return byte[].class;
            case Types.DATE:
                return java.sql.Date.class;
            case Types.TIME:
                return Time.class;
            case Types.TIMESTAMP:
                return Timestamp.class;
            case Types.CLOB:
                return Clob.class;
            case Types.BLOB:
                return Blob.class;

            // all the rest are currently not implemented and used
            default:
                return String.class;
        }
    }
}
