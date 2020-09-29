#include <jni.h>
#include "nl_cwi_monetdb_monetdbe_MonetNative.h"
#include "monetdbe.c"

JNIEXPORT jint JNICALL Java_nl_cwi_monetdb_monetdbe_MonetNative_monetdbe_1open (JNIEnv* env, jclass self, jobject j_db, jstring j_url, jobject j_opts) {
  //convert and access resources
  monetdbe_database* db = (*env)->GetDirectBufferAddress(env,j_db);
  char* const_url = (char*) (*env)->GetStringUTFChars(env,j_url,NULL);
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

JNIEXPORT jstring JNICALL Java_nl_cwi_monetdb_monetdbe_MonetNative_monetdbe_1error (JNIEnv * env, jclass self, jobject j_db) {
  monetdbe_database* db = (*env)->GetDirectBufferAddress(env,j_db);
  const char* result = (const char*) monetdbe_error(db);
  return (*env)->NewStringUTF(env,result);
}