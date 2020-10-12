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

JNIEXPORT jobject JNICALL Java_nl_cwi_monetdb_monetdbe_MonetNative_monetdbe_1open (JNIEnv* env, jclass self, jstring j_url) {
  monetdbe_database* db = malloc(sizeof(monetdbe_database));
  monetdbe_options* opts = malloc(sizeof(monetdbe_options));
  opts->memorylimit = 0;
  opts->querytimeout = 0;
  opts->sessiontimeout = 0;
  opts->nr_threads = 1;

  char* url = (char*) (*env)->GetStringUTFChars(env,j_url,NULL);
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

  char* sql = (char*) (*env)->GetStringUTFChars(env,j_sql,NULL);
  monetdbe_database db = (*env)->GetDirectBufferAddress(env,j_db);

  char* result_msg = monetdbe_query(db, sql, result, affected_rows);
  printf("Result msg: %s\n", result_msg);
  printf("Affected rows: %d\n", (*affected_rows));

  jobject resultNative = (*env)->NewDirectByteBuffer(env,(*result),sizeof(monetdbe_result));

  jclass returnClass = (*env)->FindClass(env, "Lnl/cwi/monetdb/monetdbe$NativeResult;");
  jmethodID constructor = (*env)->GetMethodID(env, returnClass, "<init>", "(Ljava/nio/ByteBuffer;I)V");
  printf("%p %d",*result,(int)(*affected_rows));
  jobject returnObject = (*env)->NewObject(env,returnClass,constructor,resultNative,(int) (*affected_rows));

  /*jclass returnArrayClass = (*env)->FindClass(env, "[Ljava/lang/Object;");
  jobjectArray returnValues = (*env)->NewObjectArray(env,2,returnArrayClass,NULL);
  (*env)->SetObjectArrayElement(env,returnValues,0,);
  //(*env)->SetObjectArrayElement(env,returnValues,1,(jobject)(*affected_rows));*/

  return returnObject;
}

JNIEXPORT jstring JNICALL Java_nl_cwi_monetdb_monetdbe_MonetNative_monetdbe_1error (JNIEnv * env, jclass self, jobject j_db) {
  monetdbe_database db = (*env)->GetDirectBufferAddress(env,j_db);
  char* result = monetdbe_error(db);
  return (*env)->NewStringUTF(env,(const char*) result);
}