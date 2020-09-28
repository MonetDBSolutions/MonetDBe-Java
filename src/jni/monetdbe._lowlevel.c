#include <jni.h>
#include "nl_cwi_monetdb_monetdbe_MonetNative.h"
#include "monetdbe.c"

JNIEXPORT jint JNICALL Java_nl_cwi_monetdb_monetdbe_MonetNative_monetdbe_1open (JNIEnv * env, jclass self, jobject j_dbhdl, jstring j_url, jobject j_opts) {
  //convert and access resources
  monetdbe_database *dbhdl = (*env)->GetDirectBufferAddress(env,j_dbhdl);
  char *url = env->GetStringUTFChars(env, j_url, NULL);
  monetdbe_options *opts = (*env)->GetDirectBufferAddress(env,j_opts);

  //call monetdbe_open
  int result = monetdbe_open(*dbhdl,*url,*opts);

  //release resources
  env->ReleaseStringUTFChars(env, j_url, url);
  return result;
}

/*
 * Class:     nl_cwi_monetdb_monetdbe_MonetNative
 * Method:    monetdbe_close
 * Signature: (Ljava/nio/ByteBuffer;)I
 */
JNIEXPORT jint JNICALL Java_nl_cwi_monetdb_monetdbe_MonetNative_monetdbe_1close (JNIEnv * env, jclass self, jobject j_dbhdl) {
  monetdbe_database *dbhdl = (*env)->GetDirectBufferAddress(env,j_dbhdl);
  int result = monetdbe_open(*dbhdl,*url,*opts);
  return result;
}