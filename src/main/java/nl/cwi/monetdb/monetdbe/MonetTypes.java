package nl.cwi.monetdb.monetdbe;

import java.math.BigDecimal;
import java.sql.*;
import java.util.Map;

public final class MonetTypes {
    //TYPE MAPPINGS
    public static final String[] monetdbeTypes = {"monetdbe_bool","monetdbe_int8_t","monetdbe_int16_t","monetdbe_int32_t","monetdbe_int64_t","monetdbe_int128_t","monetdbe_size_t","monetdbe_float","monetdbe_double","monetdbe_str","monetdbe_blob","monetdbe_date","monetdbe_time","monetdbe_timestamp","monetdbe_type_unknown"};
    public static final String[] sqlTypes = {"CHAR","VARCHAR","LONGVARCHAR","NUMERIC","DECIMAL","BOOLEAN","BIT","TINYINT","SMALLINT","INTEGER","BIGINT","REAL","FLOAT","DOUBLE","BINARY","VARBINARY","LONGVARBINARY","DATE","TIME","TIMESTAMP","CLOB","BLOB"};
    public static final Class[] javaTypes = {String.class,BigDecimal.class,Boolean.class,Short.class,Integer.class,Long.class,Float.class,Double.class,byte[].class,java.sql.Date.class,Time.class,Timestamp.class,Clob.class,Blob.class};

    /** A static Map containing the mapping between MonetDB types and Java SQL types */
    public static final java.util.Map<String, Integer> typeMapMonetdbe = new java.util.HashMap<String, Integer>();
    static {
        typeMapMonetdbe.put("monetdbe_bool", Types.BOOLEAN);
        typeMapMonetdbe.put("monetdbe_int8_t",Types.TINYINT);
        typeMapMonetdbe.put("monetdbe_int16_t", Types.SMALLINT);
        typeMapMonetdbe.put("monetdbe_int32_t", Types.INTEGER);
        typeMapMonetdbe.put("monetdbe_int64_t", Types.BIGINT);
        typeMapMonetdbe.put("monetdbe_size_t", Types.INTEGER);
        typeMapMonetdbe.put("monetdbe_float", Types.REAL);
        typeMapMonetdbe.put("monetdbe_double", Types.DOUBLE);
        typeMapMonetdbe.put("monetdbe_str", Types.VARCHAR);
        typeMapMonetdbe.put("monetdbe_blob", Types.BLOB);
        typeMapMonetdbe.put("monetdbe_date", Types.DATE);
        typeMapMonetdbe.put("monetdbe_time", Types.TIME);
        typeMapMonetdbe.put("monetdbe_timestamp", Types.TIMESTAMP);
        typeMapMonetdbe.put("monetdbe_unknown", Types.NULL);
        //TODO
        typeMapMonetdbe.put("monetdbe_int128_t", 0);
    }

    /** A static Map containing the mapping between MonetDB types and Java SQL types */
    public static final java.util.Map<String, Integer> sizeMapMonetdbe = new java.util.HashMap<String, Integer>();
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

    final static int getTypeForClass(final Class<?> cl) {
        if (cl == String.class) {
            return Types.VARCHAR;
        }
        else if (cl == Boolean.class) {
            return Types.BOOLEAN;
        }
        else if (cl == Short.class) {
            return Types.SMALLINT;
        }
        else if (cl == Integer.class) {
            return Types.INTEGER;
        }
        else if (cl == Long.class) {
            return Types.BIGINT;
        }
        else if (cl == Float.class) {
            return Types.REAL;
        }
        else if (cl == Double.class) {
            return Types.DOUBLE;
        }
        else if (cl == byte[].class) {
            return Types.BINARY;
        }
        else if (cl == java.sql.Date.class) {
            return Types.DATE;
        }
        else if (cl == Time.class) {
            return Types.TIME;
        }
        else if (cl == Timestamp.class) {
            return Types.TIMESTAMP;
        }
        else if (cl == Clob.class) {
            return Types.CLOB;
        }
        else if (cl == Blob.class) {
            return Types.BLOB;
        }
        else {
            return Types.VARCHAR;
        }
    }
}
