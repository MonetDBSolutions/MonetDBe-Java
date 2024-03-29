/* DO NOT EDIT THIS FILE - it is machine generated */
#include <jni.h>
/* Header for class org_monetdb_monetdbe_MonetNative */

#ifndef _Included_org_monetdb_monetdbe_MonetNative
#define _Included_org_monetdb_monetdbe_MonetNative
#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     org_monetdb_monetdbe_MonetNative
 * Method:    monetdbe_open
 * Signature: (Ljava/lang/String;Lorg/monetdb/monetdbe/MonetConnection;)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_org_monetdb_monetdbe_MonetNative_monetdbe_1open__Ljava_lang_String_2Lorg_monetdb_monetdbe_MonetConnection_2
  (JNIEnv *, jclass, jstring, jobject);

/*
 * Class:     org_monetdb_monetdbe_MonetNative
 * Method:    monetdbe_open
 * Signature: (Ljava/lang/String;Lorg/monetdb/monetdbe/MonetConnection;IIIILjava/lang/String;)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_org_monetdb_monetdbe_MonetNative_monetdbe_1open__Ljava_lang_String_2Lorg_monetdb_monetdbe_MonetConnection_2IIIILjava_lang_String_2
  (JNIEnv *, jclass, jstring, jobject, jint, jint, jint, jint, jstring);

/*
 * Class:     org_monetdb_monetdbe_MonetNative
 * Method:    monetdbe_open
 * Signature: (Ljava/lang/String;Lorg/monetdb/monetdbe/MonetConnection;IIIILjava/lang/String;ILjava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_org_monetdb_monetdbe_MonetNative_monetdbe_1open__Ljava_lang_String_2Lorg_monetdb_monetdbe_MonetConnection_2IIIILjava_lang_String_2ILjava_lang_String_2Ljava_lang_String_2Ljava_lang_String_2Ljava_lang_String_2
  (JNIEnv *, jclass, jstring, jobject, jint, jint, jint, jint, jstring, jint, jstring, jstring, jstring, jstring);

/*
 * Class:     org_monetdb_monetdbe_MonetNative
 * Method:    monetdbe_close
 * Signature: (Ljava/nio/ByteBuffer;)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_org_monetdb_monetdbe_MonetNative_monetdbe_1close
  (JNIEnv *, jclass, jobject);

/*
 * Class:     org_monetdb_monetdbe_MonetNative
 * Method:    monetdbe_query
 * Signature: (Ljava/nio/ByteBuffer;Ljava/lang/String;Lorg/monetdb/monetdbe/MonetStatement;ZI)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_org_monetdb_monetdbe_MonetNative_monetdbe_1query
  (JNIEnv *, jclass, jobject, jstring, jobject, jboolean, jint);

/*
 * Class:     org_monetdb_monetdbe_MonetNative
 * Method:    monetdbe_result_fetch_all
 * Signature: (Ljava/nio/ByteBuffer;II)[Lorg/monetdb/monetdbe/MonetColumn;
 */
JNIEXPORT jobjectArray JNICALL Java_org_monetdb_monetdbe_MonetNative_monetdbe_1result_1fetch_1all
  (JNIEnv *, jclass, jobject, jint, jint);

/*
 * Class:     org_monetdb_monetdbe_MonetNative
 * Method:    monetdbe_result_cleanup
 * Signature: (Ljava/nio/ByteBuffer;Ljava/nio/ByteBuffer;)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_org_monetdb_monetdbe_MonetNative_monetdbe_1result_1cleanup
  (JNIEnv *, jclass, jobject, jobject);

/*
 * Class:     org_monetdb_monetdbe_MonetNative
 * Method:    monetdbe_error
 * Signature: (Ljava/nio/ByteBuffer;)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_org_monetdb_monetdbe_MonetNative_monetdbe_1error
  (JNIEnv *, jclass, jobject);

/*
 * Class:     org_monetdb_monetdbe_MonetNative
 * Method:    monetdbe_set_autocommit
 * Signature: (Ljava/nio/ByteBuffer;I)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_org_monetdb_monetdbe_MonetNative_monetdbe_1set_1autocommit
  (JNIEnv *, jclass, jobject, jint);

/*
 * Class:     org_monetdb_monetdbe_MonetNative
 * Method:    monetdbe_get_autocommit
 * Signature: (Ljava/nio/ByteBuffer;)Z
 */
JNIEXPORT jboolean JNICALL Java_org_monetdb_monetdbe_MonetNative_monetdbe_1get_1autocommit
  (JNIEnv *, jclass, jobject);

/*
 * Class:     org_monetdb_monetdbe_MonetNative
 * Method:    monetdbe_prepare
 * Signature: (Ljava/nio/ByteBuffer;Ljava/lang/String;Lorg/monetdb/monetdbe/MonetPreparedStatement;)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_org_monetdb_monetdbe_MonetNative_monetdbe_1prepare
  (JNIEnv *, jclass, jobject, jstring, jobject);

/*
 * Class:     org_monetdb_monetdbe_MonetNative
 * Method:    monetdbe_execute
 * Signature: (Ljava/nio/ByteBuffer;Lorg/monetdb/monetdbe/MonetPreparedStatement;ZI)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_org_monetdb_monetdbe_MonetNative_monetdbe_1execute
  (JNIEnv *, jclass, jobject, jobject, jboolean, jint);

/*
 * Class:     org_monetdb_monetdbe_MonetNative
 * Method:    monetdbe_cleanup_statement
 * Signature: (Ljava/nio/ByteBuffer;Ljava/nio/ByteBuffer;)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_org_monetdb_monetdbe_MonetNative_monetdbe_1cleanup_1statement
  (JNIEnv *, jclass, jobject, jobject);

/*
 * Class:     org_monetdb_monetdbe_MonetNative
 * Method:    monetdbe_bind_bool
 * Signature: (Ljava/nio/ByteBuffer;IZ)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_org_monetdb_monetdbe_MonetNative_monetdbe_1bind_1bool
  (JNIEnv *, jclass, jobject, jint, jboolean);

/*
 * Class:     org_monetdb_monetdbe_MonetNative
 * Method:    monetdbe_bind_byte
 * Signature: (Ljava/nio/ByteBuffer;IB)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_org_monetdb_monetdbe_MonetNative_monetdbe_1bind_1byte
  (JNIEnv *, jclass, jobject, jint, jbyte);

/*
 * Class:     org_monetdb_monetdbe_MonetNative
 * Method:    monetdbe_bind_short
 * Signature: (Ljava/nio/ByteBuffer;IS)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_org_monetdb_monetdbe_MonetNative_monetdbe_1bind_1short
  (JNIEnv *, jclass, jobject, jint, jshort);

/*
 * Class:     org_monetdb_monetdbe_MonetNative
 * Method:    monetdbe_bind_int
 * Signature: (Ljava/nio/ByteBuffer;II)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_org_monetdb_monetdbe_MonetNative_monetdbe_1bind_1int
  (JNIEnv *, jclass, jobject, jint, jint);

/*
 * Class:     org_monetdb_monetdbe_MonetNative
 * Method:    monetdbe_bind_long
 * Signature: (Ljava/nio/ByteBuffer;IJ)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_org_monetdb_monetdbe_MonetNative_monetdbe_1bind_1long
  (JNIEnv *, jclass, jobject, jint, jlong);

/*
 * Class:     org_monetdb_monetdbe_MonetNative
 * Method:    monetdbe_bind_hugeint
 * Signature: (Ljava/nio/ByteBuffer;ILjava/math/BigInteger;)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_org_monetdb_monetdbe_MonetNative_monetdbe_1bind_1hugeint
  (JNIEnv *, jclass, jobject, jint, jobject);

/*
 * Class:     org_monetdb_monetdbe_MonetNative
 * Method:    monetdbe_bind_float
 * Signature: (Ljava/nio/ByteBuffer;IF)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_org_monetdb_monetdbe_MonetNative_monetdbe_1bind_1float
  (JNIEnv *, jclass, jobject, jint, jfloat);

/*
 * Class:     org_monetdb_monetdbe_MonetNative
 * Method:    monetdbe_bind_double
 * Signature: (Ljava/nio/ByteBuffer;ID)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_org_monetdb_monetdbe_MonetNative_monetdbe_1bind_1double
  (JNIEnv *, jclass, jobject, jint, jdouble);

/*
 * Class:     org_monetdb_monetdbe_MonetNative
 * Method:    monetdbe_bind_string
 * Signature: (Ljava/nio/ByteBuffer;ILjava/lang/String;)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_org_monetdb_monetdbe_MonetNative_monetdbe_1bind_1string
  (JNIEnv *, jclass, jobject, jint, jstring);

/*
 * Class:     org_monetdb_monetdbe_MonetNative
 * Method:    monetdbe_bind_blob
 * Signature: (Ljava/nio/ByteBuffer;I[BJ)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_org_monetdb_monetdbe_MonetNative_monetdbe_1bind_1blob
  (JNIEnv *, jclass, jobject, jint, jbyteArray, jlong);

/*
 * Class:     org_monetdb_monetdbe_MonetNative
 * Method:    monetdbe_bind_date
 * Signature: (Ljava/nio/ByteBuffer;IIII)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_org_monetdb_monetdbe_MonetNative_monetdbe_1bind_1date
  (JNIEnv *, jclass, jobject, jint, jint, jint, jint);

/*
 * Class:     org_monetdb_monetdbe_MonetNative
 * Method:    monetdbe_bind_time
 * Signature: (Ljava/nio/ByteBuffer;IIIII)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_org_monetdb_monetdbe_MonetNative_monetdbe_1bind_1time
  (JNIEnv *, jclass, jobject, jint, jint, jint, jint, jint);

/*
 * Class:     org_monetdb_monetdbe_MonetNative
 * Method:    monetdbe_bind_timestamp
 * Signature: (Ljava/nio/ByteBuffer;IIIIIIII)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_org_monetdb_monetdbe_MonetNative_monetdbe_1bind_1timestamp
  (JNIEnv *, jclass, jobject, jint, jint, jint, jint, jint, jint, jint, jint);

/*
 * Class:     org_monetdb_monetdbe_MonetNative
 * Method:    monetdbe_bind_decimal
 * Signature: (Ljava/nio/ByteBuffer;Ljava/lang/Object;III)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_org_monetdb_monetdbe_MonetNative_monetdbe_1bind_1decimal
  (JNIEnv *, jclass, jobject, jobject, jint, jint, jint);

/*
 * Class:     org_monetdb_monetdbe_MonetNative
 * Method:    monetdbe_bind_null
 * Signature: (Ljava/nio/ByteBuffer;ILjava/nio/ByteBuffer;I)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_org_monetdb_monetdbe_MonetNative_monetdbe_1bind_1null
  (JNIEnv *, jclass, jobject, jint, jobject, jint);

#ifdef __cplusplus
}
#endif
#endif
