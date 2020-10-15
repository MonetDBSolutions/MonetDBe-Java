#include <jni.h>
#include "nl_cwi_monetdb_monetdbe_MonetNative.h"
#include "monetdbe.c"
#include <string.h>
#include <stdio.h>

void addColumn(JNIEnv *env, jobjectArray j_data_columns, void* data, int size, int index) {
    jobject j_data = (*env)->NewDirectByteBuffer(env,data,size);
    //jobject j_data = (*env)->NewDirectByteBuffer(env,col->data,8*col->count);
    for(int i = 0; i<size;i++) {
        printf("%d\n",data[i]);
    }
    (*env)->SetObjectArrayElement(env,j_data_columns,index,j_data);
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
  if(result_msg) {
    printf("Query result msg: %s\n", result_msg);
  }

  //Query with table result
  //TODO Why is result not NULL when it's an update query like Niels said? Is checking for ncols appropriate?
  if((*result) && (*result)->ncols > 0) {
    jobject resultNative = (*env)->NewDirectByteBuffer(env,(*result),sizeof(monetdbe_result));
    jclass resultSetClass = (*env)->FindClass(env, "Lnl/cwi/monetdb/monetdbe/MonetResultSet;");
    jmethodID constructor = (*env)->GetMethodID(env, resultSetClass, "<init>", "(Lnl/cwi/monetdb/monetdbe/MonetStatement;Ljava/nio/ByteBuffer;II)V");
    jobject resultSetObject = (*env)->NewObject(env,resultSetClass,constructor,j_statement,resultNative,(*result)->nrows,(*result)->ncols);
    return resultSetObject;
  }
  //Update query
  else {
    jclass statementClass = (*env)->GetObjectClass(env, j_statement);
    jfieldID affectRowsField = (*env)->GetFieldID(env,statementClass,"updateCount","I");
    (*env)->SetIntField(env,j_statement,affectRowsField,(jint)(*affected_rows));
    return NULL;
  }
}

JNIEXPORT jobjectArray JNICALL Java_nl_cwi_monetdb_monetdbe_MonetNative_monetdbe_1result_1fetch_1all (JNIEnv * env, jclass self, jobject j_rs, jint nrows, jint ncols) {
  monetdbe_result* rs =(*env)->GetDirectBufferAddress(env,j_rs);
  monetdbe_column** column = malloc(sizeof(monetdbe_column*));
  int i,j;
  jobjectArray j_data_columns = (*env)->NewObjectArray(env,ncols,(*env)->FindClass(env, "Ljava/nio/ByteBuffer;"),NULL);

  for(i = 0; i<ncols; i++) {
    char* result_msg = monetdbe_result_fetch(rs,column,i);
    if(result_msg) {
      printf("Query result msg: %s\n", result_msg);
      return NULL;
    }
    else {
        printf("Column of type %d\n",(*column)->type);
        switch ((*column)->type) {
            case 0:;
                monetdbe_column_bool* c_bool = (monetdbe_column_bool*) (*column);
                addColumn(env,j_data_columns,c_bool->data,8*c_bool->count,i);
                break;
            case 1:;
                monetdbe_column_int8_t* c_int8_t = (monetdbe_column_int8_t*) (*column);
                addColumn(env,j_data_columns,c_int8_t->data,8*c_int8_t->count,i);
                break;
            case 2:;
                monetdbe_column_int16_t* c_int16_t = (monetdbe_column_int16_t*) (*column);
                addColumn(env,j_data_columns,c_int16_t->data,16*c_int16_t->count,i);
                break;
            case 3:;
                monetdbe_column_int32_t* c_int32_t = (monetdbe_column_int32_t*) (*column);
                addColumn(env,j_data_columns,c_int32_t->data,32*c_int32_t->count,i);
                break;
            case 4:;
                monetdbe_column_int64_t* c_int64_t = (monetdbe_column_int64_t*) (*column);
                addColumn(env,j_data_columns,c_int64_t->data,64*c_int64_t->count,i);
                break;
            case 5:;
                monetdbe_column_size_t* c_size_t = (monetdbe_column_size_t*) (*column);
                addColumn(env,j_data_columns,c_size_t->data,32*c_size_t->count,i);
                break;
            case 6:;
                monetdbe_column_float* c_float = (monetdbe_column_float*) (*column);
                addColumn(env,j_data_columns,c_float->data,32*c_float->count,i);
                break;
            case 7:;
                //TODO huge_int
                break;
            case 8:;
                monetdbe_column_double* c_double = (monetdbe_column_double*) (*column);
                addColumn(env,j_data_columns,c_double->data,64*c_double->count,i);
                break;
            //TODO Check conversions below
            case 9:;
                monetdbe_column_str* c_str = (monetdbe_column_str*) (*column);
                addColumn(env,j_data_columns,c_str->data,8*c_str->count,i);
                break;
            case 10:;
                monetdbe_column_blob* c_blob = (monetdbe_column_blob*) (*column);
                addColumn(env,j_data_columns,c_blob->data,sizeof(monetdbe_data_blob)*c_blob->count,i);
                break;
            case 11:;
                monetdbe_column_date* c_date = (monetdbe_column_date*) (*column);
                addColumn(env,j_data_columns,c_date->data,sizeof(monetdbe_data_date)*c_date->count,i);
                break;
            case 12:;
                monetdbe_column_time* c_time = (monetdbe_column_time*) (*column);
                addColumn(env,j_data_columns,c_time->data,sizeof(monetdbe_data_time)*c_time->count,i);
                break;
            case 13:;
                monetdbe_column_timestamp* c_timestamp = (monetdbe_column_timestamp*) (*column);
                addColumn(env,j_data_columns,c_timestamp->data,sizeof(monetdbe_data_timestamp)*c_timestamp->count,i);
                break;
            default:
                //TODO What should we do in this case?
                break;
        }
    }
  }
  return j_data_columns;
}

JNIEXPORT jstring JNICALL Java_nl_cwi_monetdb_monetdbe_MonetNative_monetdbe_1error (JNIEnv * env, jclass self, jobject j_db) {
  monetdbe_database db = (*env)->GetDirectBufferAddress(env,j_db);
  char* result = monetdbe_error(db);
  return (*env)->NewStringUTF(env,(const char*) result);
}