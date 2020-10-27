package nl.cwi.monetdb.monetdbe;

import java.math.BigDecimal;
import java.nio.*;
import java.sql.*;

//TODO What conversion methods do we have when a monetdbeType which is not the column's monetdbeType is requested? What types can be converted to other types?

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

    //TODO Do we need this here?
    public MonetColumn(String name, int monetdbeType, String[] varData) {
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
        if (monetdbeType ==0)  {
            return ((ByteBuffer) constData).get(row)!=0;
        }
        else {
            //TODO Check which conversions are possible
            throw new SQLException("Column is not bool value");
        }
    }

    public Short getShort(int row) throws SQLException {
        if (monetdbeType == 1 || monetdbeType == 2)  {
            return ((ByteBuffer) constData).asShortBuffer().get(row);
        }
        else {
            //TODO Check which conversions are possible
            throw new SQLException("Column is not short value");
        }
    }

    public Integer getInt(int row) throws SQLException {
        if (monetdbeType ==3 || monetdbeType == 6)  {
            return ((ByteBuffer) constData).asIntBuffer().get(row);
        }
        else {
            //TODO Check which conversions are possible
            throw new SQLException("Column is not int value");
        }
    }

    public Long getLong(int row) throws SQLException {
        if (monetdbeType ==4)  {
            return ((ByteBuffer) constData).asLongBuffer().get(row);
        }
        else {
            //TODO Check which conversions are possible
            throw new SQLException("Column is not int value");
        }
    }

    public Integer getSize(int row) throws  SQLException {
        return getInt(row);
    }

    //TODO: Check this monetdbeType, something wrong is happening
    public Float getFloat(int row) throws SQLException {
        if (monetdbeType ==7)  {
            return ((ByteBuffer) constData).asFloatBuffer().get(row);
        }
        else {
            //TODO Check which conversions are possible
            throw new SQLException("Column is not float value");
        }
    }

    public Double getDouble(int row) throws SQLException {
        if (monetdbeType ==8)  {
            return ((ByteBuffer) constData).asDoubleBuffer().get(row);
        }
        else {
            //TODO Check which conversions are possible
            throw new SQLException("Column is not double value");
        }
    }

    //Variable length types
    public String getString(int row) throws SQLException {
        if(monetdbeType ==9) {
            return (String) varData[row];
        }
        else {
            //TODO Check which conversions are possible
            throw new SQLException("Column is not string value");
        }
    }

    public byte getByte(int row) throws  SQLException {
        if(monetdbeType < 9) {
            return ((ByteBuffer) constData).get(row);
        }
        else {
            //TODO
            return 0;
        }
    }

    //TYPE MAPPINGS

    //TODO Should this be here?
    private final String[] monetdbeTypes = {"monetdbe_bool","monetdbe_int8_t","monetdbe_int16_t","monetdbe_int32_t","monetdbe_int64_t","monetdbe_int128_t","monetdbe_size_t","monetdbe_float","monetdbe_double","monetdbe_str","monetdbe_blob","monetdbe_date","monetdbe_time","monetdbe_timestamp","monetdbe_type_unknown"};
    private final String[] sqlTypes = {"CHAR","VARCHAR","LONGVARCHAR","NUMERIC","DECIMAL","BOOLEAN","BIT","TINYINT","SMALLINT","INTEGER","BIGINT","REAL","FLOAT","DOUBLE","BINARY","VARBINARY","LONGVARBINARY","DATE","TIME","TIMESTAMP","CLOB","BLOB"};
    private final Class[] javaTypes = {String.class,BigDecimal.class,Boolean.class,Short.class,Integer.class,Long.class,Float.class,Double.class,byte[].class,java.sql.Date.class,Time.class,Timestamp.class,Clob.class,Blob.class};


    /** A static Map containing the mapping between MonetDB types and Java SQL types */
    private static final java.util.Map<String, Integer> typeMap = new java.util.HashMap<String, Integer>();
    static {
        // typeMap.put("any", Integer.valueOf(Types.???));
        typeMap.put("bigint", Integer.valueOf(Types.BIGINT));
        typeMap.put("blob", Integer.valueOf(Types.BLOB));
        typeMap.put("boolean", Integer.valueOf(Types.BOOLEAN));
        typeMap.put("char", Integer.valueOf(Types.CHAR));
        typeMap.put("clob", Integer.valueOf(Types.CLOB));
        typeMap.put("date", Integer.valueOf(Types.DATE));
        typeMap.put("decimal", Integer.valueOf(Types.DECIMAL));
        typeMap.put("double", Integer.valueOf(Types.DOUBLE));
        typeMap.put("hugeint", Integer.valueOf(Types.NUMERIC));
        typeMap.put("inet", Integer.valueOf(Types.VARCHAR));
        typeMap.put("int", Integer.valueOf(Types.INTEGER));
        typeMap.put("json", Integer.valueOf(Types.VARCHAR));
        typeMap.put("month_interval", Integer.valueOf(Types.INTEGER));
        typeMap.put("oid", Integer.valueOf(Types.BIGINT));
        typeMap.put("real", Integer.valueOf(Types.REAL));
        typeMap.put("sec_interval", Integer.valueOf(Types.DECIMAL));
        typeMap.put("smallint", Integer.valueOf(Types.SMALLINT));
        typeMap.put("str", Integer.valueOf(Types.VARCHAR));
        typeMap.put("time", Integer.valueOf(Types.TIME));
        typeMap.put("timestamp", Integer.valueOf(Types.TIMESTAMP));
        typeMap.put("timestamptz", Integer.valueOf(Types.TIMESTAMP));
        typeMap.put("timetz", Integer.valueOf(Types.TIME));
        typeMap.put("tinyint", Integer.valueOf(Types.TINYINT));
        typeMap.put("url", Integer.valueOf(Types.VARCHAR));
        typeMap.put("uuid", Integer.valueOf(Types.VARCHAR));
        typeMap.put("varchar", Integer.valueOf(Types.VARCHAR));
    }

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

    final static int getSQLType(final String monetdbetype) {
        System.out.println(monetdbetype);
        return typeMapMonetdbe.get(monetdbetype);
    }

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
