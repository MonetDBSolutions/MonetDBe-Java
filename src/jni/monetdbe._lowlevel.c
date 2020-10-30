#include <jni.h>
#include "nl_cwi_monetdb_monetdbe_MonetNative.h"
#include "monetdbe.h"
#include <string.h>
#include <stdio.h>

jobjectArray parseColumnTimestamp (JNIEnv *env, void* data, int rows) {
    jobjectArray j_data = (*env)->NewObjectArray(env,rows,(*env)->FindClass(env, "Ljava/lang/String;"),NULL);
    monetdbe_data_timestamp* timestamps = (monetdbe_data_timestamp*) data;

    for(int i = 0; i < rows; i++) {
        monetdbe_data_time time = timestamps[i].time;
        monetdbe_data_date date = timestamps[i].date;
        char timestamp_str[10];
        //TODO MILLISECONDS
        //TODO HEAD ZEROS FOR ONE DIGIT TIMES
        snprintf(timestamp_str,19,"%d-%d-%d %d:%d:%d",(int)date.year,(int)date.month,(int)date.day,(int)time.hours,(int)time.minutes,(int)time.seconds);
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
        //TODO MILLISECONDS
        //TODO HEAD ZEROS FOR ONE DIGIT TIMES
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

//Free monetdbe_result (and BB?)
//Free MonetColumn name?
//Free MonetColumn strings from vardata array?
JNIEXPORT jstring JNICALL Java_nl_cwi_monetdb_monetdbe_MonetNative_monetdbe_1result_1cleanup (JNIEnv * env, jclass self, jobject j_db, jobject j_rs) {
    monetdbe_result* rs =(*env)->GetDirectBufferAddress(env,j_rs);
    monetdbe_database db = (*env)->GetDirectBufferAddress(env,j_db);
    char* result = monetdbe_cleanup_result(db,rs);
    return (*env)->NewStringUTF(env,(const char*) result);
}

void addColumnVar(JNIEnv *env, jobjectArray j_columns, void* data, char* name, int type, int rows, int index) {
    jobject j_column_object = getColumnJavaVar(env,data,name,type,rows);
    (*env)->SetObjectArrayElement(env,j_columns,index,j_column_object);
}

void addColumnConst(JNIEnv *env, jobjectArray j_columns, void* data, char* name, int type, int size, int index) {
    jobject j_column_object = getColumnJavaConst(env,data,name,type,size);
    (*env)->SetObjectArrayElement(env,j_columns,index,j_column_object);
}

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
  if(strcmp(url,":memory:")==0) {
    printf("Memory\n");
    fflush(stdout);
    result = monetdbe_open(db,NULL,opts);
  }
  else {
    result = monetdbe_open(db,url,opts);
  }
  (*env)->ReleaseStringUTFChars(env, j_url, url);

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

JNIEXPORT jobject JNICALL Java_nl_cwi_monetdb_monetdbe_MonetNative_monetdbe_1query (JNIEnv * env, jclass self, jobject j_db, jstring j_sql, jobject j_statement) {
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

  //Query with table result
  //TODO Why is result not NULL when it's an update query like Niels said? Is checking for ncols appropriate?
  if((*result) && (*result)->ncols > 0) {
    jobject resultNative = (*env)->NewDirectByteBuffer(env,(*result),sizeof(monetdbe_result));
    jstring resultSetName = (*env)->NewStringUTF(env,(const char*) (*result)->name);
    jclass resultSetClass = (*env)->FindClass(env, "Lnl/cwi/monetdb/monetdbe/MonetResultSet;");
    jmethodID constructor = (*env)->GetMethodID(env, resultSetClass, "<init>", "(Lnl/cwi/monetdb/monetdbe/MonetStatement;Ljava/nio/ByteBuffer;IILjava/lang/String;)V");
    jobject resultSetObject = (*env)->NewObject(env,resultSetClass,constructor,j_statement,resultNative,(*result)->nrows,(*result)->ncols,resultSetName);
    free(affected_rows);
    //TODO What happens if we free result here? Does the DirectByteBuffer reference die?
    return resultSetObject;
  }
  //Update query
  else {
    jclass statementClass = (*env)->GetObjectClass(env, j_statement);
    jfieldID affectRowsField = (*env)->GetFieldID(env,statementClass,"updateCount","I");
    (*env)->SetIntField(env,j_statement,affectRowsField,(jint)(*affected_rows));
    free(affected_rows);
    free(result);
    return NULL;
  }
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
        //printf("Column of type %d\n",(*column)->type);
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
                //TODO huge_int
                break;
            case 6:;
                monetdbe_column_size_t* c_size_t = (monetdbe_column_size_t*) (*column);
                addColumnConst(env,j_columns,c_size_t->data,c_size_t->name,5,32*c_size_t->count,i);
                break;
            case 7:;
                monetdbe_column_float* c_float = (monetdbe_column_float*) (*column);
                addColumnConst(env,j_columns,c_float->data,c_float->name,6,32*c_float->count,i);
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

JNIEXPORT jstring JNICALL Java_nl_cwi_monetdb_monetdbe_MonetNative_monetdbe_1error (JNIEnv * env, jclass self, jobject j_db) {
  monetdbe_database db = (*env)->GetDirectBufferAddress(env,j_db);
  char* result = monetdbe_error(db);
  return (*env)->NewStringUTF(env,(const char*) result);
}