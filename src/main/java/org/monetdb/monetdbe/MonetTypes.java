package org.monetdb.monetdbe;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;

//TODO Organize this better, remove redudant methods, verify if everything is correct
public final class MonetTypes {
    public static final String[] monetdbeTypes = {"monetdbe_bool","monetdbe_int8_t","monetdbe_int16_t","monetdbe_int32_t","monetdbe_int64_t","monetdbe_int128_t","monetdbe_size_t","monetdbe_float","monetdbe_double","monetdbe_str","monetdbe_blob","monetdbe_date","monetdbe_time","monetdbe_timestamp","monetdbe_type_unknown"};
    public static final String[] sqlDefaultTypeNames = {"BOOLEAN","TINYINT","SMALLINT","INTEGER","BIGINT","INTEGER","REAL","DOUBLE","VARCHAR","BLOB","DATE","TIME","TIMESTAMP","NULL"};

    public static final String[] sqlTypes = {"CHAR","VARCHAR","LONGVARCHAR","NUMERIC","DECIMAL","BOOLEAN","BIT","TINYINT","SMALLINT","INTEGER","BIGINT","REAL","FLOAT","DOUBLE","BINARY","VARBINARY","LONGVARBINARY","DATE","TIME","TIMESTAMP","CLOB","BLOB"};
    public static final Class[] javaClasss = {String.class,BigDecimal.class,Boolean.class,Short.class,Integer.class,Long.class,Float.class,Double.class,byte[].class,java.sql.Date.class,Time.class,Timestamp.class,Clob.class,Blob.class};

    final static String getMonetTypeString(final int monetdbetype) {
        return monetdbeTypes[monetdbetype];
    }

    final static int getMonetTypeFromTypeString (final String monetdbetype) {
        for (int i = 0; i < monetdbeTypes.length; i++) {
            if (monetdbeTypes[i].equals(monetdbetype)) {
                return i;
            }
        }
        return -1;
    }

    /** A static Map containing the mapping between MonetDB types and Java SQL types */
    public static final java.util.Map<Integer, Integer> typeMapMonetToSQL = new java.util.HashMap<Integer, Integer>();
    static {
        typeMapMonetToSQL.put(0, Types.BOOLEAN);
        typeMapMonetToSQL.put(1,Types.TINYINT);
        typeMapMonetToSQL.put(2, Types.SMALLINT);
        typeMapMonetToSQL.put(3, Types.INTEGER);
        typeMapMonetToSQL.put(4, Types.BIGINT);
        typeMapMonetToSQL.put(6, Types.INTEGER);
        typeMapMonetToSQL.put(7, Types.REAL);
        typeMapMonetToSQL.put(8, Types.DOUBLE);
        typeMapMonetToSQL.put(9, Types.VARCHAR);
        typeMapMonetToSQL.put(10, Types.BLOB);
        typeMapMonetToSQL.put(11, Types.DATE);
        typeMapMonetToSQL.put(12, Types.TIME);
        typeMapMonetToSQL.put(13, Types.TIMESTAMP);
        typeMapMonetToSQL.put(14, Types.NULL);
        //TODO Verify this
        typeMapMonetToSQL.put(5, Types.NUMERIC);
    }

    final static int getSQLTypeFromMonet(final int monetdbetype) {
        return typeMapMonetToSQL.get(monetdbetype);
    }

    final static int getSQLTypeFromMonet(final String monetdbetype) {
        return typeMapMonetToSQL.get(getMonetTypeFromTypeString(monetdbetype));
    }

    /** A static Map containing the mapping between Java SQL types and MonetDB types */
    public static final java.util.Map<Integer, Integer> typeMapSQLToMonet = new java.util.HashMap<Integer, Integer>();
    static {
        typeMapSQLToMonet.put(Types.BOOLEAN,0);
        typeMapSQLToMonet.put(Types.BIT,0);
        typeMapSQLToMonet.put(Types.TINYINT,1);
        typeMapSQLToMonet.put(Types.SMALLINT,2);
        typeMapSQLToMonet.put(Types.INTEGER,3);
        typeMapSQLToMonet.put(Types.BIGINT,4);
        typeMapSQLToMonet.put(Types.REAL,7);
        typeMapSQLToMonet.put(Types.FLOAT,7);
        typeMapSQLToMonet.put(Types.DOUBLE,8);
        typeMapSQLToMonet.put(Types.CHAR,9);
        typeMapSQLToMonet.put(Types.VARCHAR,9);
        typeMapSQLToMonet.put(Types.LONGVARCHAR,9);
        typeMapSQLToMonet.put(Types.NCHAR,9);
        typeMapSQLToMonet.put(Types.LONGNVARCHAR,9);
        typeMapSQLToMonet.put(Types.BLOB,10);
        typeMapSQLToMonet.put(Types.DATE,11);
        typeMapSQLToMonet.put(Types.TIME,12);
        typeMapSQLToMonet.put(Types.TIMESTAMP,13);

        typeMapSQLToMonet.put(Types.NUMERIC,5);
        typeMapSQLToMonet.put(Types.DECIMAL,5);
        typeMapSQLToMonet.put(Types.VARBINARY,10);
        typeMapSQLToMonet.put(Types.LONGVARBINARY,10);
        typeMapMonetToSQL.put(Types.BINARY,10);
    }

    final static String getMonetTypeStringFromSQL(final int sqltype) {
        return getMonetTypeString(typeMapSQLToMonet.get(sqltype));
    }

    final static int getMonetTypeIntFromSQL(final int sqltype) {
        return typeMapSQLToMonet.get(sqltype);
    }

    public static final java.util.Map<String, Integer> typeMapSQLNameToSQLInt = new java.util.HashMap<String, Integer>();
    static {
        typeMapSQLNameToSQLInt.put("BOOLEAN",Types.BOOLEAN);
        typeMapSQLNameToSQLInt.put("BIT",Types.BIT);
        typeMapSQLNameToSQLInt.put("TINYINT",Types.TINYINT);
        typeMapSQLNameToSQLInt.put("SMALLINT",Types.SMALLINT);
        typeMapSQLNameToSQLInt.put("INTEGER",Types.INTEGER);
        typeMapSQLNameToSQLInt.put("BIGINT",Types.BIGINT);
        typeMapSQLNameToSQLInt.put("REAL",Types.REAL);
        typeMapSQLNameToSQLInt.put("FLOAT",Types.FLOAT);
        typeMapSQLNameToSQLInt.put("DOUBLE",Types.DOUBLE);
        typeMapSQLNameToSQLInt.put("CHAR",Types.CHAR);
        typeMapSQLNameToSQLInt.put("VARCHAR",Types.VARCHAR);
        typeMapSQLNameToSQLInt.put("LONGVARCHAR",Types.LONGVARCHAR);
        typeMapSQLNameToSQLInt.put("NCHAR",Types.NCHAR);
        typeMapSQLNameToSQLInt.put("LONGNVARCHAR",Types.LONGNVARCHAR);
        typeMapSQLNameToSQLInt.put("BLOB",Types.BLOB);
        typeMapSQLNameToSQLInt.put("DATE",Types.DATE);
        typeMapSQLNameToSQLInt.put("TIME",Types.TIME);
        typeMapSQLNameToSQLInt.put("TIMESTAMP",Types.TIMESTAMP);
        typeMapSQLNameToSQLInt.put("NUMERIC",Types.NUMERIC);
        typeMapSQLNameToSQLInt.put("DECIMAL",Types.DECIMAL);
        typeMapSQLNameToSQLInt.put("VARBINARY",Types.VARBINARY);
        typeMapSQLNameToSQLInt.put("LONGVARBINARY",Types.LONGNVARCHAR);
        typeMapSQLNameToSQLInt.put("BINARY",Types.BINARY);
    }

    final static int getSQLIntFromSQLName (final String sqlTypeName) {
        return typeMapSQLNameToSQLInt.get(sqlTypeName);
    }

    final static int getMonetTypeIntFromSQLName(final String sqlTypeName) {
        return getMonetTypeIntFromSQL(getSQLIntFromSQLName(sqlTypeName));
    }

    final static String getDefaultSQLTypeNameFromMonet(final int monetdbetype) {
        return sqlDefaultTypeNames[monetdbetype];
    }

    /** A static Map containing the mapping between MonetDB types and Java SQL types */
    public static final java.util.Map<Integer, Integer> sizeMapMonetInt = new java.util.HashMap<Integer, Integer>();
    static {
        sizeMapMonetInt.put(0, 8);
        sizeMapMonetInt.put(1, 8);
        sizeMapMonetInt.put(2, 16);
        sizeMapMonetInt.put(3, 32);
        sizeMapMonetInt.put(4, 64);
        sizeMapMonetInt.put(5, 128);
        sizeMapMonetInt.put(6, 32);
        sizeMapMonetInt.put(7, 32);
        sizeMapMonetInt.put(8, 64);
        sizeMapMonetInt.put(9, 0);
        sizeMapMonetInt.put(10, 0);
        sizeMapMonetInt.put(11, 10);
        sizeMapMonetInt.put(12, 12);
        sizeMapMonetInt.put(13, 23);
        sizeMapMonetInt.put(14, 0);
    }

    //For static size types
    final static int getMonetSize(final int monetdbetype) {
        return sizeMapMonetInt.get(monetdbetype);
    }

    final static int getMonetSize(final String monetdbetype) {
        return sizeMapMonetString.get(monetdbetype);
    }

    final static Class<?> getClassForSQLType(final int sqlType) {
        switch(sqlType) {
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
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
            default:
                return String.class;
        }
    }

    final static Class<?> getClassForMonetType(final int monetdbeType) { return getClassForSQLType(getSQLTypeFromMonet(monetdbeType));}

    final static int getDefaultSQLTypeForClass(final Class<?> javaClass) {
        if (javaClass == String.class) {
            return Types.VARCHAR;
        }
        else if (javaClass == Boolean.class) {
            return Types.BOOLEAN;
        }
        else if (javaClass == Short.class) {
            return Types.SMALLINT;
        }
        else if (javaClass == Integer.class) {
            return Types.INTEGER;
        }
        else if (javaClass == Long.class) {
            return Types.BIGINT;
        }
        else if (javaClass == Float.class) {
            return Types.REAL;
        }
        else if (javaClass == Double.class) {
            return Types.DOUBLE;
        }
        else if (javaClass == byte[].class) {
            return Types.BINARY;
        }
        else if (javaClass == Date.class) {
            return Types.DATE;
        }
        else if (javaClass == Time.class) {
            return Types.TIME;
        }
        else if (javaClass == Timestamp.class) {
            return Types.TIMESTAMP;
        }
        else if (javaClass == Clob.class) {
            return Types.CLOB;
        }
        else if (javaClass == Blob.class) {
            return Types.BLOB;
        }
        else if (javaClass == BigDecimal.class) {
            return Types.NUMERIC;
        }
        else {
            return Types.VARCHAR;
        }
    }
    
    final static boolean convertTojavaClass (final int monetdbetype, final Class<?> javaClass) {
        if (javaClass == String.class) {
            return (monetdbetype != 10 && monetdbetype >= 0 && monetdbetype <= 13);
        }
        else if (javaClass == Boolean.class || javaClass == Short.class || javaClass == Integer.class || javaClass == Long.class || javaClass == Float.class || javaClass == Double.class || javaClass == BigDecimal.class) {
            return (monetdbetype >= 0 && monetdbetype <= 9);
        }
        else if (javaClass == Date.class) {
            return (monetdbetype == 9 || monetdbetype == 11 || monetdbetype == 13);
        }
        else if (javaClass == Time.class) {
            return (monetdbetype == 9 || monetdbetype == 12 || monetdbetype == 13);
        }
        else if (javaClass == Timestamp.class) {
            return (monetdbetype >= 9 && monetdbetype <= 13);
        }
        else if (javaClass == Blob.class || javaClass == byte[].class) {
            return (monetdbetype == 10);
        }
        else if (javaClass == Clob.class) {
            return (monetdbetype == 9);
        }
        else if (javaClass == BigInteger.class) {
            return (monetdbetype == 4 || monetdbetype == 5 || monetdbetype == 9);
        }
        else {
            return false;
        }
    }

    /** A static Map containing the mapping between MonetDB types and Java SQL types */
    public static final java.util.Map<String, Integer> sizeMapMonetString = new java.util.HashMap<String, Integer>();
    static {
        sizeMapMonetString.put("monetdbe_bool", 8);
        sizeMapMonetString.put("monetdbe_int8_t", 8);
        sizeMapMonetString.put("monetdbe_int16_t", 16);
        sizeMapMonetString.put("monetdbe_int32_t", 32);
        sizeMapMonetString.put("monetdbe_int64_t", 64);
        sizeMapMonetString.put("monetdbe_int128_t", 128);
        sizeMapMonetString.put("monetdbe_size_t", 32);
        sizeMapMonetString.put("monetdbe_float", 32);
        sizeMapMonetString.put("monetdbe_double", 64);
        sizeMapMonetString.put("monetdbe_str", 0);
        sizeMapMonetString.put("monetdbe_blob", 0);
        sizeMapMonetString.put("monetdbe_date", 10);
        sizeMapMonetString.put("monetdbe_time", 8);
        sizeMapMonetString.put("monetdbe_timestamp", 19);
        sizeMapMonetString.put("monetdbe_unknown", 0);
    }

    public static boolean isSigned (int sqlType) {
        switch (sqlType) {
            case Types.NUMERIC:
            case Types.DECIMAL:
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.REAL:
            case Types.FLOAT:
            case Types.DOUBLE:
            case Types.BIGINT:
                return true;
            case Types.BIT: // we don't use type BIT, it's here for completeness
            case Types.BOOLEAN:
            case Types.DATE:
            case Types.TIME:
            case Types.TIMESTAMP:
            default:
                return false;
        }
    }
}
