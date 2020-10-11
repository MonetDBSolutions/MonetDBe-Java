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
	jbyte* bytes = (jbyte*) (*env)->GetByteArrayElements(env, array, NULL);
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

  printf("Open result: %d\n", result);

  if (result != 0) {
     char* error = monetdbe_error(db);
     printf("Error: %s\n",error);
     fflush(stdout);
     return NULL;
  }
  else {
    //return (*env)->NewDirectByteBuffer(env,(*db),(jlong) sizeof(monetdbe_database));
    return (*env)->NewDirectByteBuffer(env,(*db),0);
  }
}


/*JNIEXPORT jint JNICALL Java_nl_cwi_monetdb_monetdbe_MonetNative_monetdbe_1open (JNIEnv* env, jclass self, jobject j_db, jstring j_url, jobject j_opts) {
  /*monetdbe_database* db = (*env)->GetDirectBufferAddress(env,j_db);
  char* url = (char*) (*env)->GetStringUTFChars(env,j_url,NULL);
  printf("%s\n", url);
  fflush(stdout);
  monetdbe_options* opts = (*env)->GetDirectBufferAddress(env,j_opts);

  monetdbe_database* db;
  monetdbe_options* opts;
  char* url = (char*) (*env)->GetStringUTFChars(env,j_url,NULL);

  int result = monetdbe_open(db,url,opts);
  (*env)->ReleaseStringUTFChars(env, j_url, url);

  j_db = (*env)->NewDirectByteBuffer(env,db,sizeof(monetdbe_database));
  return result;
}*/

/*JNIEXPORT jint JNICALL Java_nl_cwi_monetdb_monetdbe_MonetNative_monetdbe_1open (JNIEnv* env, jclass self, jobject j_db, jbyteArray j_url, jobject j_opts) {
  monetdbe_database* db = (*env)->GetDirectBufferAddress(env,j_db);
  char* url = byte_array_to_string(env,j_url);
  monetdbe_options* opts = (*env)->GetDirectBufferAddress(env,j_opts);
  int result = monetdbe_open(db,url,opts);
  (*env)->ReleaseStringUTFChars(env, j_url, url);
  return result;
}*/

JNIEXPORT jint JNICALL Java_nl_cwi_monetdb_monetdbe_MonetNative_monetdbe_1close (JNIEnv * env, jclass self, jobject j_db) {
  monetdbe_database* db = (*env)->GetDirectBufferAddress(env,j_db);
  int result = monetdbe_close(db);
  return result;
}

JNIEXPORT jbyteArray JNICALL Java_nl_cwi_monetdb_monetdbe_MonetNative_monetdbe_1error (JNIEnv * env, jclass self, jobject j_db) {
  monetdbe_database* db = (*env)->GetDirectBufferAddress(env,j_db);
  char* result = monetdbe_error(db);
  jbyteArray byte_array = string_to_byte_array(env,result);
  return byte_array;
}