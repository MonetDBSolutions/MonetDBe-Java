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


JNIEXPORT jint JNICALL Java_nl_cwi_monetdb_monetdbe_MonetNative_monetdbe_1open (JNIEnv* env, jclass self, jobject j_db, jbyteArray j_url, jobject j_opts) {
  monetdbe_database* db = (*env)->GetDirectBufferAddress(env,j_db);
  /*char* url = (char*) (*env)->GetStringUTFChars(env,j_url,NULL);
  printf("%s", url);
  fflush(stdout);*/
  //const char* const_url = (*env)->GetStringUTFChars(env,j_url,0);
  //char* url = malloc(strlen(const_url));
  //strcpy(url,const_url);
  char* url = byte_array_to_string(env,j_url);
  printf("%s", url);
  fflush(stdout);
  monetdbe_options* opts = (*env)->GetDirectBufferAddress(env,j_opts);

  //call monetdbe_open
  int result = monetdbe_open(db,url,opts);

  //release resources
  (*env)->ReleaseStringUTFChars(env, j_url, url);
  return result;
}

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