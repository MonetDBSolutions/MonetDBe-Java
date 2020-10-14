#include <jni.h>
#include "nl_cwi_monetdb_monetdbe_MonetNative.h"
#include "monetdbe.c"
#include <string.h>
#include <stdio.h>

char* byte_array_to_string(JNIEnv *env, jbyteArray array_j) {
	int len = (*env)->GetArrayLength(env,array_j);
	//Bytearray len + NULL terminator
	char* string = malloc(len+1);
	jbyte* array = (jbyte*) (*env)->GetByteArrayElements(env, array_j, NULL);

	/*for (int i = 0; i < len; i++) {
		string[i] = array[i];
	}*/

	memcpy(string,array,len);
	string[len] = 0;

	(*env)->ReleaseByteArrayElements(env, array_j, array, 0);
	return string;
}

jbyteArray string_to_byte_array(JNIEnv *env, char* string) {
	int len = strlen(string);
	jbyteArray array = (*env)->NewByteArray(env,len);
	//jbyte* bytes = (jbyte*) (*env)->GetByteArrayElements(env, array, NULL);
	(*env)->SetByteArrayRegion(env,array,0,len,(jbyte*)string);
    return array;
}

JNIEXPORT jobject JNICALL Java_nl_cwi_monetdb_monetdbe_MonetNative_monetdbe_1open__Ljava_lang_String_2 (JNIEnv* env, jclass self, jstring j_url) {
  return Java_nl_cwi_monetdb_monetdbe_MonetNative_monetdbe_1open__Ljava_lang_String_2IIII(env,self,j_url,0,0,0,0);
}

JNIEXPORT jobject JNICALL Java_nl_cwi_monetdb_monetdbe_MonetNative_monetdbe_1open__Ljava_lang_String_2IIII (JNIEnv * env, jclass self, jstring j_url, jint j_sessiontimeout, jint j_querytimeout, jint j_memorylimit, jint j_nr_threads) {
  monetdbe_database* db = malloc(sizeof(monetdbe_database));
  monetdbe_options* opts = malloc(sizeof(monetdbe_options));
  opts->memorylimit = j_memorylimit;
  opts->querytimeout = j_querytimeout;
  opts->sessiontimeout = j_sessiontimeout;
  opts->nr_threads = j_nr_threads;

  char* url = (char*) (*env)->GetStringUTFChars(env,j_url,NULL);
  printf("%s %c\n",url,url[strlen(url)]);
  if(url[strlen(url)] == '\0') { printf("Correct\n");}
  int result = monetdbe_open(db,url,opts);
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

JNIEXPORT jobject JNICALL Java_nl_cwi_monetdb_monetdbe_MonetNative_monetdbe_1query (JNIEnv * env, jclass self, jobject j_db, jstring j_sql) {
  monetdbe_result** result = malloc(sizeof(monetdbe_result*));
  monetdbe_cnt* affected_rows = malloc(sizeof(monetdbe_cnt));
  (*affected_rows) = -1;
  char* sql = (char*) (*env)->GetStringUTFChars(env,j_sql,NULL);
  monetdbe_database db = (*env)->GetDirectBufferAddress(env,j_db);

  char* result_msg = monetdbe_query(db, sql, result, affected_rows);
  if(result_msg) {
    printf("Query result msg: %s\n", result_msg);
  }
  jobject resultNative = (*env)->NewDirectByteBuffer(env,(*result),sizeof(monetdbe_result));
  jclass returnClass = (*env)->FindClass(env, "Lnl/cwi/monetdb/monetdbe/NativeResult;");

  //jmethodID constructor = (*env)->GetMethodID(env, returnClass, "<init>", "(Ljava/nio/ByteBuffer;I)V");
  //jobject returnObject = (*env)->NewObject(env,returnClass,constructor,resultNative,(int) (*affected_rows));

  jobject returnObject = NULL;
  //TODO What is the condition for being a query instead on an update? Some results? Can queries not have empty results?
  if((*result) && (*result)->ncols > 0) {
    jmethodID constructor = (*env)->GetMethodID(env, returnClass, "<init>", "(Ljava/nio/ByteBuffer;II)V");
    returnObject = (*env)->NewObject(env,returnClass,constructor,resultNative,(*result)->nrows,(*result)->ncols);
  }
  else if((*affected_rows)){
    jmethodID constructor = (*env)->GetMethodID(env, returnClass, "<init>", "(I)V");
    returnObject = (*env)->NewObject(env,returnClass,constructor,(int) (*affected_rows));
  }
  return returnObject;
}



JNIEXPORT jobject JNICALL Java_nl_cwi_monetdb_monetdbe_MonetNative_monetdbe_1result_1fetch_1all (JNIEnv * env, jclass self, jobject j_rs, jint nrows, jint ncols) {
  monetdbe_result* rs =(*env)->GetDirectBufferAddress(env,j_rs);
  monetdbe_column** column = malloc(sizeof(monetdbe_column*));
  monetdbe_column* columns [ncols];
  char* types[ncols];
  char* type_dict[]= {"monetdbe_bool", "monetdbe_int8_t", "monetdbe_int16_t", "monetdbe_int32_t", "monetdbe_int64_t", "monetdbe_int128_t", "monetdbe_size_t", "monetdbe_float", "monetdbe_double", "monetdbe_str", "monetdbe_blob,monetdbe_date", "monetdbe_time", "monetdbe_timestamp", "monetdbe_type_unknown"};
  int i,j;

  for(i = 0; i<ncols; i++) {
    char* result_msg = monetdbe_result_fetch(rs,column,i);
    if(result_msg) {
      printf("Query result msg: %s\n", result_msg);
    }

    if((*column)->type == 0) {
        monetdbe_column_bool* col = (monetdbe_column_bool*) (*column);
        printf("%d",col->is_null);
    }

    if((*column)->type == 2) {
        monetdbe_column_int16_t* col = (monetdbe_column_int16_t*) (*column);
        printf("Int 16 Count: %d\n",col->count);
    }

    if((*column)->type == 3) {
        monetdbe_column_int32_t* col = (monetdbe_column_int32_t*) (*column);
        printf("Int 32 Count: %d\nInt 32 Scale : %d\n",col->count,col->scale);
        printf("Values:\n");
        for (j=0;j<col->count;j++) {
          printf("%d (%d null), ",col->data[j],col->is_null(col->data+j));
        }

    }

    columns[i] = (*column);
    types[i] = type_dict[(*column)->type];
    printf("Column %s of type %s and count %d\n",(*column)->name,types[i],(*column)->count);
    fflush(stdout);
  }


  return NULL;
}

JNIEXPORT jstring JNICALL Java_nl_cwi_monetdb_monetdbe_MonetNative_monetdbe_1error (JNIEnv * env, jclass self, jobject j_db) {
  monetdbe_database db = (*env)->GetDirectBufferAddress(env,j_db);
  char* result = monetdbe_error(db);
  return (*env)->NewStringUTF(env,(const char*) result);
}