package nl.cwi.monetdb.monetdbe;

import java.nio.*;
import java.sql.SQLException;

//TODO What conversion methods do we have when a type which is not the column's type is requested? What types can be converted to other types?

public class MonetColumn {
    private Buffer constData;
    private Object[] varData;
    private String name;
    private int type;
    private String typeName;

    //TODO Should this be here?
    private final String[] monetdbeTypes = {"monetdbe_bool","monetdbe_int8_t","monetdbe_int16_t","monetdbe_int32_t","monetdbe_int 64_t","monetdbe_int128_t","monetdbe_size_t","monetdbe_float","monetdbe_double","monetdbe_str","monetdbe_blob","monetdbe_date","monetdbe_time","monetdbe_timestamp","monetdbe_type_unknown"};

    public MonetColumn(String name, int type) {
        this.name = name;
        this.type = type;
        this.typeName = monetdbeTypes[type];
    }

    public MonetColumn(String name, int type, ByteBuffer constData) {
        this.name = name;
        this.type = type;
        this.typeName = monetdbeTypes[type];
        this.constData = constData.order(ByteOrder.LITTLE_ENDIAN);
    }

    public MonetColumn(String name, int type, Object[] varData) {
        this.name = name;
        this.type = type;
        this.typeName = monetdbeTypes[type];
        this.varData = varData;
    }

    //TODO Do we need this here? If we use the C string[] for calling the Object[] constructor, JNI throws an expection
    public MonetColumn(String name, int type, String[] varData) {
        this.name = name;
        this.type = type;
        this.typeName = monetdbeTypes[type];
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

    public int getType() {
        return type;
    }

    public String getTypeName() {
        return typeName;
    }

    //Constant length types
    public boolean getBoolean(int row) throws SQLException {
        if (type==0)  {
            return ((ByteBuffer) constData).get(row)!=0;
        }
        else {
            //TODO Check which conversions are possible
            throw new SQLException("Column is not bool value");
        }
    }

    public Short getShort(int row) throws SQLException {
        if (type==2)  {
            return ((ByteBuffer) constData).asShortBuffer().get(row);
        }
        else {
            //TODO Check which conversions are possible
            throw new SQLException("Column is not short value");
        }
    }

    public Integer getInt(int row) throws SQLException {
        if (type==3 || type == 6)  {
            return ((ByteBuffer) constData).asIntBuffer().get(row);
        }
        else {
            //TODO Check which conversions are possible
            throw new SQLException("Column is not int value");
        }
    }

    public Long getLong(int row) throws SQLException {
        if (type==4)  {
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

    //TODO: Check this type, something wrong is happening
    public Float getFloat(int row) throws SQLException {
        if (type==7)  {
            return ((ByteBuffer) constData).asFloatBuffer().get(row);
        }
        else {
            //TODO Check which conversions are possible
            throw new SQLException("Column is not float value");
        }
    }

    public Double getDouble(int row) throws SQLException {
        if (type==8)  {
            return ((ByteBuffer) constData).asDoubleBuffer().get(row);
        }
        else {
            //TODO Check which conversions are possible
            throw new SQLException("Column is not double value");
        }
    }

    //Variable length types
    public String getString(int row) throws SQLException {
        if(type==9) {
            return (String) varData[row];
        }
        else {
            //TODO Check which conversions are possible
            throw new SQLException("Column is not string value");
        }
    }

    public byte getByte(int row) throws  SQLException {
        if(type < 9) {
            return ((ByteBuffer) constData).get(row);
        }
        else {
            //TODO
            return 0;
        }
    }
}
