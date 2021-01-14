#include <jni.h>
#include "nl_cwi_monetdb_monetdbe_MonetNative.h"
#include "monetdbe.h"
#include <string.h>
#include <stdio.h>


JNIEXPORT jobject JNICALL Java_nl_cwi_monetdb_monetdbe_MonetNative_monetdbe_1open__Ljava_lang_String_2 (JNIEnv* env, jclass self, jstring j_url) {
  return Java_nl_cwi_monetdb_monetdbe_MonetNative_monetdbe_1open__Ljava_lang_String_2IIII(env,self,j_url,0,0,0,0);
}

JNIEXPORT jobject JNICALL Java_nl_cwi_monetdb_monetdbe_MonetNative_monetdbe_1open__Ljava_lang_String_2IIII (JNIEnv * env, jclass self, jstring j_url, jint j_sessiontimeout, jint j_querytimeout, jint j_memorylimit, jint j_nr_threads) {
  monetdbe_database* db = malloc(sizeof(monetdbe_database));
  monetdbe_options* opts = malloc(sizeof(monetdbe_options));
  opts->memorylimit = (int) j_memorylimit;
  opts->querytimeout = (int) j_querytimeout;
  opts->sessiontimeout = (int) j_sessiontimeout;
  opts->nr_threads = (int) j_nr_threads;
  opts->remote = NULL;
  opts->mapi_server = NULL;

  char* url = (char*) (*env)->GetStringUTFChars(env,j_url,NULL);
  int result;
  result = monetdbe_open(db,url,opts);

  if (result != 0) {
     char* error = monetdbe_error(*db);
     printf("Error in monetdbe_open: %s\n",error);
     fflush(stdout);
  }
  return (*env)->NewDirectByteBuffer(env,(*db),sizeof(monetdbe_database));
}

JNIEXPORT jint JNICALL Java_nl_cwi_monetdb_monetdbe_MonetNative_monetdbe_1close (JNIEnv * env, jclass self, jobject j_db) {
  monetdbe_database db = (*env)->GetDirectBufferAddress(env,j_db);
  return monetdbe_close(db);
}

jobject returnResult (JNIEnv * env, jobject j_statement, jboolean largeUpdate, monetdbe_result** result, monetdbe_cnt* affected_rows) {
  //Query with table result
  if((*result) && (*result)->ncols > 0) {
    jobject resultNative = (*env)->NewDirectByteBuffer(env,(*result),sizeof(monetdbe_result));
    jstring resultSetName = (*env)->NewStringUTF(env,(const char*) (*result)->name);
    jclass resultSetClass = (*env)->FindClass(env, "Lnl/cwi/monetdb/monetdbe/MonetResultSet;");
    jmethodID constructor = (*env)->GetMethodID(env, resultSetClass, "<init>", "(Lnl/cwi/monetdb/monetdbe/MonetStatement;Ljava/nio/ByteBuffer;IILjava/lang/String;)V");
    jobject resultSetObject = (*env)->NewObject(env,resultSetClass,constructor,j_statement,resultNative,(*result)->nrows,(*result)->ncols,resultSetName);
    free(affected_rows);
    return resultSetObject;
  }
  //Update query
  else {
    jclass statementClass = (*env)->GetObjectClass(env, j_statement);
    if (largeUpdate) {
        jfieldID affectRowsField = (*env)->GetFieldID(env,statementClass,"largeUpdateCount","J");
        (*env)->SetLongField(env,j_statement,affectRowsField,(jlong)(*affected_rows));
    }
    else {
        jfieldID affectRowsField = (*env)->GetFieldID(env,statementClass,"updateCount","I");
        (*env)->SetIntField(env,j_statement,affectRowsField,(jint)(*affected_rows));
    }
    free(affected_rows);
    free(result);
    return NULL;
  }
}

JNIEXPORT jobject JNICALL Java_nl_cwi_monetdb_monetdbe_MonetNative_monetdbe_1query (JNIEnv * env, jclass self, jobject j_db, jstring j_sql, jobject j_statement, jboolean largeUpdate) {
  monetdbe_result** result = malloc(sizeof(monetdbe_result*));
  monetdbe_cnt* affected_rows = malloc(sizeof(monetdbe_cnt));
  char* sql = (char*) (*env)->GetStringUTFChars(env,j_sql,NULL);
  monetdbe_database db = (*env)->GetDirectBufferAddress(env,j_db);

  char* result_msg = monetdbe_query(db, sql, result, affected_rows);
  (*env)->ReleaseStringUTFChars(env,j_sql,sql);
  if(result_msg) {
    printf("Query result msg: %s\n", result_msg);
    fflush(stdout);
  }
  return returnResult(env, j_statement, largeUpdate, result, affected_rows);
}

//TODO Time and Timestamp parse functions aren't returning the ms value
jobjectArray parseColumnTimestamp (JNIEnv *env, void* data, int rows) {
    jobjectArray j_data = (*env)->NewObjectArray(env,rows,(*env)->FindClass(env, "Ljava/lang/String;"),NULL);
    monetdbe_data_timestamp* timestamps = (monetdbe_data_timestamp*) data;

    for(int i = 0; i < rows; i++) {
        monetdbe_data_time time = timestamps[i].time;
        monetdbe_data_date date = timestamps[i].date;
        char timestamp_str[23];
        //TODO HEAD ZEROS FOR ONE DIGIT TIMES
        snprintf(timestamp_str,23,"%d-%d-%d %d:%d:%d.%d",(int)date.year,(int)date.month,(int)date.day,(int)time.hours,(int)time.minutes,(int)time.seconds,(int)time.ms);
        jobject j_timestamp = (*env)->NewStringUTF(env,(const char*) timestamp_str);
        (*env)->SetObjectArrayElement(env,j_data,i,j_timestamp);
        fflush(stdout);
    }
    return j_data;
}

jobjectArray parseColumnTime (JNIEnv *env, void* data, int rows) {
    jobjectArray j_data = (*env)->NewObjectArray(env,rows,(*env)->FindClass(env, "Ljava/lang/String;"),NULL);
    monetdbe_data_time* times = (monetdbe_data_time*) data;

    for(int i = 0; i < rows; i++) {
        char time_str[8];
        //TODO HEAD ZEROS FOR ONE DIGIT TIMES
        //TODO MS? Time.valueOf() doesn't accept ms
        snprintf(time_str,8,"%d:%d:%d",(int)times[i].hours,(int)times[i].minutes,(int)times[i].seconds);
        jobject j_time = (*env)->NewStringUTF(env,(const char*) time_str);
        (*env)->SetObjectArrayElement(env,j_data,i,j_time);
    }
    return j_data;
}

jobjectArray parseColumnDate (JNIEnv *env, void* data, int rows) {
    jobjectArray j_data = (*env)->NewObjectArray(env,rows,(*env)->FindClass(env, "Ljava/lang/String;"),NULL);
    monetdbe_data_date* dates = (monetdbe_data_date*) data;

    for(int i = 0; i < rows; i++) {
        char date_str[10];
        snprintf(date_str,10,"%d-%d-%d",(int)dates[i].year,(int)dates[i].month,(int)dates[i].day);
        jobject j_date = (*env)->NewStringUTF(env,(const char*) date_str);
        (*env)->SetObjectArrayElement(env,j_data,i,j_date);
        //printf("Column Date: %d-%d-%d\n", dates[i].year,dates[i].month,dates[i].day);
    }
    return j_data;
}

jobjectArray parseColumnString (JNIEnv *env, void* data, int rows) {
    jobjectArray j_data = (*env)->NewObjectArray(env,rows,(*env)->FindClass(env, "Ljava/lang/String;"),NULL);
    char** strings = (char**) data;

    for(int i = 0; i < rows; i++) {
        jobject j_string = (*env)->NewStringUTF(env,(const char*) (strings[i]));
        (*env)->SetObjectArrayElement(env,j_data,i,j_string);
    }
    return j_data;
}

jobject getColumnJavaVar (JNIEnv *env, void* data, char* name, int type, int rows) {
    jobjectArray j_data;

    if(type == 9) {
        j_data = parseColumnString(env,data,rows);
    }
    else if(type == 11) {
        j_data = parseColumnDate(env,data,rows);
    }
    else if(type == 12) {
        j_data = parseColumnTime(env,data,rows);
    }
    else if(type == 13) {
        j_data = parseColumnTimestamp(env,data,rows);
    }

    jstring j_name = (*env)->NewStringUTF(env,(const char*) name);
    jclass j_column = (*env)->FindClass(env, "Lnl/cwi/monetdb/monetdbe/MonetColumn;");
    jmethodID constructor = (*env)->GetMethodID(env, j_column, "<init>", "(Ljava/lang/String;I[Ljava/lang/Object;)V");
    return (*env)->NewObject(env,j_column,constructor,j_name,(jint) type,j_data);
}

jobject getColumnJavaConst(JNIEnv *env, void* data, char* name, int type, int size) {
    jobject j_data = (*env)->NewDirectByteBuffer(env,data,size);
    jstring j_name = (*env)->NewStringUTF(env,(const char*) name);

    jclass j_column = (*env)->FindClass(env, "Lnl/cwi/monetdb/monetdbe/MonetColumn;");
    jmethodID constructor = (*env)->GetMethodID(env, j_column, "<init>", "(Ljava/lang/String;ILjava/nio/ByteBuffer;)V");
    return (*env)->NewObject(env,j_column,constructor,j_name,(jint) type,j_data);
}

void addColumnVar(JNIEnv *env, jobjectArray j_columns, void* data, char* name, int type, int rows, int index) {
    jobject j_column_object = getColumnJavaVar(env,data,name,type,rows);
    (*env)->SetObjectArrayElement(env,j_columns,index,j_column_object);
}

void addColumnConst(JNIEnv *env, jobjectArray j_columns, void* data, char* name, int type, int size, int index) {
    jobject j_column_object = getColumnJavaConst(env,data,name,type,size);
    (*env)->SetObjectArrayElement(env,j_columns,index,j_column_object);
}

JNIEXPORT jobjectArray JNICALL Java_nl_cwi_monetdb_monetdbe_MonetNative_monetdbe_1result_1fetch_1all (JNIEnv * env, jclass self, jobject j_rs, jint nrows, jint ncols) {
  monetdbe_result* rs =(*env)->GetDirectBufferAddress(env,j_rs);
  monetdbe_column** column = malloc(sizeof(monetdbe_column*));
  jobjectArray j_columns = (*env)->NewObjectArray(env,ncols,(*env)->FindClass(env, "Lnl/cwi/monetdb/monetdbe/MonetColumn;"),NULL);

  for(int i = 0; i<ncols; i++) {
    char* result_msg = monetdbe_result_fetch(rs,column,i);
    if(result_msg) {
      printf("Query result msg: %s\n", result_msg);
      return NULL;
    }
    else {
        //printf("Column %d of type %d\n",i,(*column)->type);
        switch ((*column)->type) {
            case 0:;
                monetdbe_column_bool* c_bool = (monetdbe_column_bool*) (*column);
                addColumnConst(env,j_columns,c_bool->data,c_bool->name,0,8*c_bool->count,i);
                break;
            case 1:;
                monetdbe_column_int8_t* c_int8_t = (monetdbe_column_int8_t*) (*column);
                addColumnConst(env,j_columns,c_int8_t->data,c_int8_t->name,1,8*c_int8_t->count,i);
                break;
            case 2:;
                monetdbe_column_int16_t* c_int16_t = (monetdbe_column_int16_t*) (*column);
                addColumnConst(env,j_columns,c_int16_t->data,c_int16_t->name,2,16*c_int16_t->count,i);
                break;
            case 3:;
                monetdbe_column_int32_t* c_int32_t = (monetdbe_column_int32_t*) (*column);
                addColumnConst(env,j_columns,c_int32_t->data,c_int32_t->name,3,32*c_int32_t->count,i);
                break;
            case 4:;
                monetdbe_column_int64_t* c_int64_t = (monetdbe_column_int64_t*) (*column);
                addColumnConst(env,j_columns,c_int64_t->data,c_int64_t->name,4,64*c_int64_t->count,i);
                break;
            case 5:;
                monetdbe_column_int128_t* c_int128_t = (monetdbe_column_int64_t*) (*column);
                addColumnConst(env,j_columns,c_int128_t->data,c_int128_t->name,5,128*c_int128_t->count,i);
                break;
            case 6:;
                monetdbe_column_size_t* c_size_t = (monetdbe_column_size_t*) (*column);
                addColumnConst(env,j_columns,c_size_t->data,c_size_t->name,6,32*c_size_t->count,i);
                break;
            case 7:;
                monetdbe_column_float* c_float = (monetdbe_column_float*) (*column);
                addColumnConst(env,j_columns,c_float->data,c_float->name,7,32*c_float->count,i);
                break;
            case 8:;
                monetdbe_column_double* c_double = (monetdbe_column_double*) (*column);
                addColumnConst(env,j_columns,c_double->data,c_double->name,8,64*c_double->count,i);
                break;
            case 9:;
                monetdbe_column_str* c_str = (monetdbe_column_str*) (*column);
                addColumnVar(env,j_columns,c_str->data,c_str->name,9,c_str->count,i);
                break;
            case 10:;
                monetdbe_column_blob* c_blob = (monetdbe_column_blob*) (*column);
                //addColumn(env,j_columns,c_blob->data,c_blob->name,10,sizeof(monetdbe_data_blob)*c_blob->count,i);
                break;
            case 11:;
                monetdbe_column_date* c_date = (monetdbe_column_date*) (*column);
                addColumnVar(env,j_columns,c_date->data,c_date->name,11,c_date->count,i);
                break;
            case 12:;
                monetdbe_column_time* c_time = (monetdbe_column_time*) (*column);
                addColumnVar(env,j_columns,c_time->data,c_time->name,12,c_time->count,i);
                break;
            case 13:;
                monetdbe_column_timestamp* c_timestamp = (monetdbe_column_timestamp*) (*column);
                addColumnVar(env,j_columns,c_timestamp->data,c_timestamp->name,13,c_timestamp->count,i);
                break;
            default:
                //TODO What should we do in this case?
                break;
        }
    }
  }
  //TODO Should we free the monetdbe_result here?
  return j_columns;
}

JNIEXPORT jstring JNICALL Java_nl_cwi_monetdb_monetdbe_MonetNative_monetdbe_1result_1cleanup (JNIEnv * env, jclass self, jobject j_db, jobject j_rs) {
    //Free monetdbe_result (and BB?)
    //Free MonetColumn name?
    //Free MonetColumn strings from vardata array?
    monetdbe_result* rs =(*env)->GetDirectBufferAddress(env,j_rs);
    monetdbe_database db = (*env)->GetDirectBufferAddress(env,j_db);
    char* result = monetdbe_cleanup_result(db,rs);
    return (*env)->NewStringUTF(env,(const char*) result);
}

JNIEXPORT jstring JNICALL Java_nl_cwi_monetdb_monetdbe_MonetNative_monetdbe_1error (JNIEnv * env, jclass self, jobject j_db) {
  monetdbe_database db = (*env)->GetDirectBufferAddress(env,j_db);
  char* result = monetdbe_error(db);
  return (*env)->NewStringUTF(env,(const char*) result);
}

JNIEXPORT jstring JNICALL Java_nl_cwi_monetdb_monetdbe_MonetNative_monetdbe_1set_1autocommit (JNIEnv * env, jclass self, jobject j_db, jint j_auto_commit) {
    monetdbe_database db = (*env)->GetDirectBufferAddress(env,j_db);
    char* result = monetdbe_set_autocommit(db,(int) j_auto_commit);
    return (*env)->NewStringUTF(env,(const char*) result);
}

JNIEXPORT jboolean JNICALL Java_nl_cwi_monetdb_monetdbe_MonetNative_monetdbe_1get_1autocommit (JNIEnv * env, jclass self, jobject j_db) {
    monetdbe_database db = (*env)->GetDirectBufferAddress(env,j_db);
    int result;
    char* result_msg = monetdbe_get_autocommit(db, &result);
    if(result_msg) {
        printf("Set_autocommit result msg: %s\n", result_msg);
        return -1;
    }
    else {
        return (jboolean) result == 1;
    }
}

JNIEXPORT jobject JNICALL Java_nl_cwi_monetdb_monetdbe_MonetNative_monetdbe_1prepare (JNIEnv * env, jclass self, jobject j_db, jstring j_sql, jobject j_statement) {
    monetdbe_database db = (*env)->GetDirectBufferAddress(env,j_db);
    monetdbe_statement** stmt = malloc(sizeof(monetdbe_statement*));
    char* sql = (char*) (*env)->GetStringUTFChars(env,j_sql,NULL);

    char* result = monetdbe_prepare(db,sql,stmt);
    if(result) {
        printf("Prepare: %s\n",result);
        fflush(stdout);
    }

    //Set parameter number
    //TODO Set parameter types
    jclass statementClass = (*env)->GetObjectClass(env, j_statement);
    jfieldID paramsField = (*env)->GetFieldID(env,statementClass,"nParams","I");
    (*env)->SetIntField(env,j_statement,paramsField,(jint)((*stmt)->nparam));

    (*env)->ReleaseStringUTFChars(env,j_sql,sql);
    return (*env)->NewDirectByteBuffer(env,(*stmt),sizeof(monetdbe_statement));
}

jstring bind_parsed_data (JNIEnv * env, jobject j_stmt, void* parsed_data, int parameter_nr) {
    if (parameter_nr > 0) {
        //JDBC indexes parameter numbers at 1, MonetDB at 0
        parameter_nr -= 1;
    }
    else {
        return (*env)->NewStringUTF(env,(const char*) "Parameter number is not valid.");
    }
    monetdbe_statement* stmt = (*env)->GetDirectBufferAddress(env,j_stmt);
    char* result = monetdbe_bind(stmt,parsed_data,(int)parameter_nr);

    if(result) {
        printf("Bind: %s\n",result);
        fflush(stdout);
    }
    else {
        printf("Bind sucessful\n");
        fflush(stdout);
    }
    return (*env)->NewStringUTF(env,(const char*) result);
}

//TODO Rethink this method, shouldn't use the monetdbe_type in the condition, as a monetdbe_type can have multiple associated JDBC types
JNIEXPORT jstring JNICALL Java_nl_cwi_monetdb_monetdbe_MonetNative_monetdbe_1bind (JNIEnv * env, jclass self, jobject j_stmt, jobject j_data, jint type, jint parameter_nr) {
    if ((*env)->IsSameObject(env, j_data, NULL)) {
        //TODO Is this correct? Am I giving a pointer to a NULL value?
        return bind_parsed_data(env,j_stmt,(void*)0,(int)parameter_nr);
    }
    //Non-NULL
    else {
        jclass param_class = (*env)->GetObjectClass(env, j_data);
        if (type == 0) {
            bool bind_data = (bool) (*env)->CallBooleanMethod(env,j_data,(*env)->GetMethodID(env,param_class,"booleanValue","()Z"));
            return bind_parsed_data(env,j_stmt,&bind_data,(int)parameter_nr);
        }
        else if (type == 1 || type == 2) {
            short bind_data = (short) (*env)->CallShortMethod(env,j_data,(*env)->GetMethodID(env,param_class,"shortValue","()S"));
            return bind_parsed_data(env,j_stmt,&bind_data,(int)parameter_nr);
        }
        else if (type == 3 || type == 6) {
            int bind_data = (int) (*env)->CallIntMethod(env,j_data,(*env)->GetMethodID(env,param_class,"intValue","()I"));
            return bind_parsed_data(env,j_stmt,&bind_data,(int)parameter_nr);
        }
        else if (type == 4) {
            long bind_data = (long) (*env)->CallLongMethod(env,j_data,(*env)->GetMethodID(env,param_class,"longValue","()L"));
            return bind_parsed_data(env,j_stmt,&bind_data,(int)parameter_nr);
        }
        else if (type == 5) {
            //TODO Parse a BigDecimal/BigInteger to int128
        }
        else if (type == 7) {
            float bind_data = (float) (*env)->CallFloatMethod(env,j_data,(*env)->GetMethodID(env,param_class,"floatValue","()F"));
            return bind_parsed_data(env,j_stmt,&bind_data,(int)parameter_nr);
        }
        else if (type == 8) {
            double bind_data = (double) (*env)->CallDoubleMethod(env,j_data,(*env)->GetMethodID(env,param_class,"doubleValue","()D"));
            return bind_parsed_data(env,j_stmt,&bind_data,(int)parameter_nr);
        }
        else if (type == 9) {
            char* bind_data = (char*) (*env)->GetStringUTFChars(env,j_data,NULL);
            return bind_parsed_data(env,j_stmt,bind_data,(int)parameter_nr);
        }
        else if (type == 10) {
            //TODO Blob
            return NULL;
        }
        return NULL;
        //Date types are handled in other functions
    }
}

//TODO Fix these functions (problem with data types)
JNIEXPORT jstring JNICALL Java_nl_cwi_monetdb_monetdbe_MonetNative_monetdbe_1bind_1date (JNIEnv * env, jclass self, jobject j_stmt, jint parameter_nr, jint year, jint month, jint day) {
    monetdbe_data_date* date_bind = malloc(sizeof(monetdbe_data_date));
    date_bind->year = (short) year;
    date_bind->month = (unsigned char) month;
    date_bind->day = (unsigned char) day;
    printf("Parsed Date: %hi-%d-%d\n", date_bind->year,date_bind->month,date_bind->day);
    return bind_parsed_data(env,j_stmt,date_bind,(int)parameter_nr);
}

JNIEXPORT jstring JNICALL Java_nl_cwi_monetdb_monetdbe_MonetNative_monetdbe_1bind_1time (JNIEnv * env, jclass self, jobject j_stmt, jint parameter_nr, jint hours, jint minutes, jint seconds, jint ms) {
    monetdbe_data_time* time_bind = malloc(sizeof(monetdbe_data_time));
    time_bind->hours = (unsigned char) hours;
    time_bind->minutes = (unsigned char) minutes;
    time_bind->seconds = (unsigned char) seconds;
    time_bind->ms = (unsigned int) ms;
    printf("Parsed Time: %d:%d:%d.%d\n", (int)time_bind->hours,(int)time_bind->minutes,(int)time_bind->seconds,(int)time_bind->ms);
    return bind_parsed_data(env,j_stmt,time_bind,(int)parameter_nr);
}

JNIEXPORT jstring JNICALL Java_nl_cwi_monetdb_monetdbe_MonetNative_monetdbe_1bind_1timestamp (JNIEnv * env, jclass self, jobject j_stmt, jint parameter_nr, jint year, jint month, jint day, jint hours, jint minutes, jint seconds, jint ms) {
    monetdbe_data_timestamp* timestamp_bind = malloc(sizeof(monetdbe_data_timestamp));
    (timestamp_bind->date).year = (short) year;
    (timestamp_bind->date).month = (unsigned char) month;
    (timestamp_bind->date).day = (unsigned char) day;
    (timestamp_bind->time).hours = (unsigned char) hours;
    (timestamp_bind->time).minutes = (unsigned char) minutes;
    (timestamp_bind->time).seconds = (unsigned char) seconds;
    (timestamp_bind->time).ms = (unsigned int) ms;
    printf("Parsed Timestamp: %d-%d-%d %d:%d:%d.%d\n", (int)(timestamp_bind->date).year,(int)(timestamp_bind->date).month,(int)(timestamp_bind->date).day,(int)(timestamp_bind->time).hours,(int)(timestamp_bind->time).minutes,(int)(timestamp_bind->time).seconds,(int)(timestamp_bind->time).ms);
    return bind_parsed_data(env,j_stmt,timestamp_bind,(int)parameter_nr);
}

JNIEXPORT jobject JNICALL Java_nl_cwi_monetdb_monetdbe_MonetNative_monetdbe_1execute (JNIEnv * env, jclass self, jobject j_stmt, jobject j_statement, jboolean largeUpdate) {
    monetdbe_statement* stmt = (*env)->GetDirectBufferAddress(env,j_stmt);
    monetdbe_result** result = malloc(sizeof(monetdbe_result*));
    monetdbe_cnt* affected_rows = malloc(sizeof(monetdbe_cnt));

    char* result_msg = monetdbe_execute(stmt,result,affected_rows);
    if(result_msg) {
        printf("Query result msg: %s\n", result_msg);
        fflush(stdout);
        return NULL;
    }
    //TODO Verify that messages are only sent if it's an error
    else {
        return returnResult(env, j_statement, largeUpdate, result, affected_rows);
    }
}

JNIEXPORT jstring JNICALL Java_nl_cwi_monetdb_monetdbe_MonetNative_monetdbe_1cleanup_1statement (JNIEnv * env, jclass self, jobject j_db, jobject j_stmt) {
    monetdbe_database db = (*env)->GetDirectBufferAddress(env,j_db);
    monetdbe_statement* stmt = (*env)->GetDirectBufferAddress(env,j_stmt);
    char* result = monetdbe_cleanup_statement(db,stmt);
    printf("%s",result);
    return (*env)->NewStringUTF(env,(const char*) result);
}