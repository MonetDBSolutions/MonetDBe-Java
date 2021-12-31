package org.monetdb.monetdbe;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;

/**
 * Helper class to convert between MonetDBe types, JDBC SQL types and Java classes.
 */
//TODO Look into getMonetTypeStringFromGDKType()
public class MonetTypes {
    static java.util.Map<Integer, Integer> typeMapMonetToSQL = new java.util.HashMap<Integer, Integer>();
    static {
        typeMapMonetToSQL.put(0, Types.BOOLEAN);
        typeMapMonetToSQL.put(1, Types.TINYINT);
        typeMapMonetToSQL.put(2, Types.SMALLINT);
        typeMapMonetToSQL.put(3, Types.INTEGER);
        typeMapMonetToSQL.put(4, Types.BIGINT);
        typeMapMonetToSQL.put(5, Types.NUMERIC);
        typeMapMonetToSQL.put(6, Types.INTEGER);
        typeMapMonetToSQL.put(7, Types.REAL);
        typeMapMonetToSQL.put(8, Types.DOUBLE);
        typeMapMonetToSQL.put(9, Types.VARCHAR);
        typeMapMonetToSQL.put(10, Types.BLOB);
        typeMapMonetToSQL.put(11, Types.DATE);
        typeMapMonetToSQL.put(12, Types.TIME);
        typeMapMonetToSQL.put(13, Types.TIMESTAMP);
        typeMapMonetToSQL.put(14, Types.NULL);
    }

    /**
     * Conversion between MonetDBe types (int) and SQL types (int)
     * @param monetdbetype MonetDBe type (int) to convert
     * @return SQL type (int)
     */
    protected static int getSQLTypeFromMonet(final int monetdbetype) {
        return typeMapMonetToSQL.get(monetdbetype);
    }

    static final String[] sqlDefaultTypeNames = {"BOOLEAN","TINYINT","SMALLINT","INTEGER","BIGINT","INTEGER","REAL","DOUBLE","VARCHAR","BLOB","DATE","TIME","TIMESTAMP","NULL"};

    /**
     * Conversion between MonetDBe types (int) and SQL types (String)
     * @param monetdbetype MonetDBe type (int) to convert
     * @return SQL type (String)
     */
    protected static String getSQLTypeNameFromMonet(final int monetdbetype) {
        return sqlDefaultTypeNames[monetdbetype];
    }

    static final java.util.Map<Integer, Integer> typeMapSQLToMonet = new java.util.HashMap<Integer, Integer>();
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
        typeMapSQLToMonet.put(Types.NVARCHAR,9);
        typeMapSQLToMonet.put(Types.LONGNVARCHAR,9);
        typeMapSQLToMonet.put(Types.CLOB,9);
        typeMapSQLToMonet.put(Types.BLOB,10);
        typeMapSQLToMonet.put(Types.BINARY,10);
        typeMapSQLToMonet.put(Types.VARBINARY,10);
        typeMapSQLToMonet.put(Types.LONGVARBINARY,10);
        typeMapSQLToMonet.put(Types.DATE,11);
        typeMapSQLToMonet.put(Types.TIME,12);
        typeMapSQLToMonet.put(Types.TIMESTAMP,13);
        typeMapSQLToMonet.put(Types.NULL,14);
        typeMapSQLToMonet.put(Types.OTHER,14);

        typeMapSQLToMonet.put(Types.DECIMAL,4);
        typeMapSQLToMonet.put(Types.NUMERIC,4);
    }

    /**
     * Conversion between SQL types (int) and MonetDBe types (int).
     * DECIMAL and NUMERIC types are returned as MonetDBe type 4, but the MonetDBe type for DECIMAL can vary.
     *
     * @param sqltype SQL type (int) to convert
     * @return MonetDBe type (int)
     */
    protected static int getMonetTypeFromSQL(final int sqltype) {
        return typeMapSQLToMonet.getOrDefault(sqltype,14);
    }

    /**
     * Conversion between SQL types (String) and MonetDBe types (int)
     * @param sqlTypeName SQL type (String) to convert
     * @return MonetDBe type (int)
     */
    protected static int getMonetTypeIntFromSQLName(final String sqlTypeName) {
        return getMonetTypeFromSQL(getSQLIntFromSQLName(sqlTypeName));
    }

    /**
     * Conversion between SQL types (int) and Java classes (Class)
     * @param sqlType SQL type (int) to convert
     * @return Java class
     */
    protected static Class<?> getClassForSQLType(final int sqlType) {
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
            case Types.BIT:
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
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return byte[].class;
            case Types.DATE:
                return Date.class;
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

    /**
     * Conversion between Java classes (Class) and SQL types (int)
     * @param javaClass Java class to convert
     * @return Default SQL type (int) for Java class
     */
    protected static int getDefaultSQLTypeForClass(final Class<?> javaClass) {
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

    /**
     * Determines if the MonetDBe type can be converted to the Java class
     * @param monetdbetype MonetDBe type to convert
     * @param javaClass Java class to be converted to
     * @return true if the type can be converted to the class, false otherwise
     */
    protected static boolean convertTojavaClass (final int monetdbetype, final Class<?> javaClass) {
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

    /**
     * Conversion between MonetDBe type (int) and Java classes (Class)
     * @param monetdbeType MonetDBe type (int) to convert
     * @return Java class
     */
    protected static Class<?> getClassForMonetType(final int monetdbeType) { return getClassForSQLType(getSQLTypeFromMonet(monetdbeType));}

    //Other utilities
    //TODO The unknown type is incorrect (unknown)
    //TODO The size_t type is incorrect (size)
    //MonetDB GDK types
    private static final String[] monetdbGDKTypes = {"bit","bte","sht","int","lng","hge","size","flt","dbl","str","blob","date","daytime","timestamp","unknown"};
    //MonetDBe types
    private static final String[] monetdbeTypes = {"monetdbe_bool","monetdbe_int8_t","monetdbe_int16_t","monetdbe_int32_t","monetdbe_int64_t","monetdbe_int128_t","monetdbe_size_t","monetdbe_float","monetdbe_double","monetdbe_str","monetdbe_blob","monetdbe_date","monetdbe_time","monetdbe_timestamp","monetdbe_type_unknown"};


    /**
     * Returns the MonetDBe string type for a MonetDB GDK type
     * @param monetdbgdktype MonetDB GDK type
     * @return Name of the MonetDBe type
     */
    protected static String getMonetTypeStringFromGDKType(final String monetdbgdktype) {
        for (int i = 0; i < monetdbGDKTypes.length; i++)
            if (monetdbGDKTypes[i].equals(monetdbgdktype))
                return monetdbeTypes[i];
        return "monetdbe_type_unknown";
    }

    /**
     * Returns the MonetDBe int type for a MonetDB GDK type
     * @param monetdbgdktype MonetDB GDK type
     * @return Int value of the MonetDBe type
     */
    protected static int getMonetTypeFromGDKType(final String monetdbgdktype) {
        for (int i = 0; i < monetdbGDKTypes.length; i++)
            if (monetdbGDKTypes[i].equals(monetdbgdktype))
                return i;
        //Unknown type
        return 13;
    }

    /**
     * Returns the MonetDB GDK type for a MonetDBe int type
     * @param monetdbetype MonetDBe type
     * @return MonetDB GDK type
     */
    protected static String getGDKTypeFromMonetType(final int monetdbetype) {
        return monetdbGDKTypes[monetdbetype];
    }

    /**
     * Returns the String name for the MonetDBe type (int)
     * @param monetdbetype MonetDBe type (int)
     * @return Name of the MonetDBe type
     */
    protected static String getMonetTypeString(final int monetdbetype) {
        return monetdbeTypes[monetdbetype];
    }

    /**
     * Returns the size of the MonetDBe type in bytes. Only works for static lenght types.
     * @param monetdbetype MonetDBe type (int)
     * @return Size of the MonetDBe type
     */
    protected static int getMonetSize(final int monetdbetype) {
        if (monetdbetype >= 9)
            return 0;
        else
            return sizeMapMonet.get(monetdbetype);
    }

    static final java.util.Map<Integer, Integer> sizeMapMonet = new java.util.HashMap<Integer, Integer>();
    static {
        sizeMapMonet.put(0, 1);
        sizeMapMonet.put(1, 1);
        sizeMapMonet.put(2, 2);
        sizeMapMonet.put(3, 4);
        sizeMapMonet.put(4, 8);
        sizeMapMonet.put(5, 16);
        sizeMapMonet.put(6, 4);
        sizeMapMonet.put(7, 4);
        sizeMapMonet.put(8, 8);
    }

    //SQL string name to SQL type integer
    static final java.util.Map<String, Integer> typeMapSQLNameToSQLInt = new java.util.HashMap<String, Integer>();
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
        typeMapSQLNameToSQLInt.put("BINARY",Types.BINARY);
        typeMapSQLNameToSQLInt.put("VARBINARY",Types.VARBINARY);
        typeMapSQLNameToSQLInt.put("LONGVARBINARY",Types.LONGVARBINARY);
        typeMapSQLNameToSQLInt.put("CLOB",Types.CLOB);
        typeMapSQLNameToSQLInt.put("NULL",Types.NULL);
        typeMapSQLNameToSQLInt.put("OTHER",Types.OTHER);
    }

    /**
     * Returns the SQL type int from a SQL type name (String)
     * @param sqlTypeName SQL type (String)
     * @return SQL type (int)
     */
    protected static int getSQLIntFromSQLName (final String sqlTypeName) {
        return typeMapSQLNameToSQLInt.get(sqlTypeName);
    }

    /**
     * Returns if the SQL type is signed.
     * @param sqlType SQL type (int)
     * @return true if the SQL type can be signed, false otherwise
     */
    protected static boolean isSigned (int sqlType) {
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
            case Types.BIT:
            case Types.BOOLEAN:
            case Types.DATE:
            case Types.TIME:
            case Types.TIMESTAMP:
            default:
                return false;
        }
    }
}
