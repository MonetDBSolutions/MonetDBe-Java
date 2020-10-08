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
	memcpy(array,string,len);

	jbyte* bytes = (jbyte*) (*env)->GetByteArrayElements(env, array, NULL);
    for (int i = 0; i < len; i++) {
        printf("%b\n", bytes[i]);
    }
    fflush(stdout);

    return array;
}

void console_printf(const char *fmt,...)
{
    int fd = open("/dev/console", O_WRONLY);
    char buffer[1000];
    if (fd < 0)
        return;

    va_list ap;
    va_start(ap, fmt);
    vsprintf(buffer, fmt, ap);
    va_end(ap);

    write(fd, buffer, strlen(buffer));
    close(fd);
}

JNIEXPORT jint JNICALL Java_nl_cwi_monetdb_monetdbe_MonetNative_monetdbe_1open (JNIEnv* env, jclass self, jobject j_db, jbyteArray j_url, jobject j_opts) {
  //convert and access resources
  monetdbe_database* db = (*env)->GetDirectBufferAddress(env,j_db);
  //char* url = (char*) (*env)->GetStringUTFChars(env,j_url,NULL);
  //const char* const_url = (*env)->GetStringUTFChars(env,j_url,0);
  //char* url = malloc(strlen(const_url));
  //strcpy(url,const_url);
  char* url = byte_array_to_string(env,j_url);
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
  char* result = monetdbe_error(db);
  printf("%d %s\n",strlen(result),result);
  //printf("%s\n", result);
  char* r = strdup(r,result);
  //printf("%s\n", r);

  //jbyteArray byte_array = string_to_byte_array(env,result);

  //char *buf = (char*)malloc(10);
  //strcpy(buf, "123456789");

  jstring result_string = (*env)->NewStringUTF(env,(const char*) result);

  fflush(stdout);
  return result_string;
}