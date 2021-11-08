#include <jni.h>
#include "org_monetdb_monetdbe_MonetNative.h"
#include "monetdbe.h"
#include <string.h>
#include <stdio.h>

//TODO Look into frees

void set_options_log (JNIEnv *env, monetdbe_options *opts, jstring j_logfile) {
#ifdef MONETDBE_VERSION
    if (j_logfile != NULL)
    {
        const char *logfile = (*env)->GetStringUTFChars(env, j_logfile, NULL);
        opts->trace_file = logfile;
    }
    else {
        opts->trace_file = NULL;
    }
#endif
}

void set_options_mapi(JNIEnv *env, monetdbe_options *opts, jstring j_port, jstring j_sock)
{
#ifdef MONETDBE_VERSION
    //MAPI server
    if (j_port != NULL && j_sock != NULL)
    {
        const char *port = (*env)->GetStringUTFChars(env, j_port, NULL);
        const char *sock = (*env)->GetStringUTFChars(env, j_sock, NULL);
        monetdbe_mapi_server *mapi = malloc(sizeof(monetdbe_mapi_server));
        mapi->port = port;
        mapi->usock = sock;
        opts->mapi_server = mapi;
    }
#endif
}

void set_options_remote(JNIEnv *env, monetdbe_options *opts, jstring j_host, jint j_port, jstring j_database, jstring j_user, jstring j_password)
{
#ifdef MONETDBE_VERSION
    //Remote proxy
    if (j_host != NULL && j_port > 0 && j_user != NULL && j_password != NULL)
    {
        const char *user = (*env)->GetStringUTFChars(env, j_user, NULL);
        const char *password = (*env)->GetStringUTFChars(env, j_password, NULL);
        const char *host = (*env)->GetStringUTFChars(env, j_host, NULL);
        const char *database = (*env)->GetStringUTFChars(env, j_database, NULL);

        monetdbe_remote *remote = malloc(sizeof(monetdbe_remote));
        remote->host = host;
        remote->port = (int)j_port;
        remote->username = user;
        remote->password = password;
        remote->database = database;
        remote->lang = NULL;
        opts->remote = remote;
    }
#endif
}

monetdbe_options *set_options(JNIEnv *env, jint j_sessiontimeout, jint j_querytimeout, jint j_memorylimit, jint j_nr_threads, jstring j_logfile)
{
    monetdbe_options *opts = malloc(sizeof(monetdbe_options));
    opts->memorylimit = (int)j_memorylimit;
    opts->querytimeout = (int)j_querytimeout;
    opts->sessiontimeout = (int)j_sessiontimeout;
    opts->nr_threads = (int)j_nr_threads;
#ifdef MONETDBE_VERSION
    opts->remote = NULL;
    opts->mapi_server = NULL;
    set_options_log(env,opts,j_logfile);
#endif
    return opts;
}

jstring open_db(JNIEnv *env, jstring j_url, monetdbe_options *opts, jobject j_connection)
{
    const char *url = NULL;
    if (j_url != NULL)
    {
        url = (*env)->GetStringUTFChars(env, j_url, NULL);
    }

    monetdbe_database *db = malloc(sizeof(monetdbe_database));
    int error_code = monetdbe_open(db, (char *)url, opts);
    if (url != NULL)
    {
        (*env)->ReleaseStringUTFChars(env, j_url, url);
    }
    if (error_code != 0)
    {
        char *error_msg = monetdbe_error(*db);
        return (*env)->NewStringUTF(env, (const char *)error_msg);
    }
    else
    {
        //Set DB reference in Connection object that called the method
        jclass connectionClass = (*env)->GetObjectClass(env, j_connection);
        jfieldID dbNativeField = (*env)->GetFieldID(env, connectionClass, "dbNative", "Ljava/nio/ByteBuffer;");
        (*env)->SetObjectField(env, j_connection, dbNativeField, (*env)->NewDirectByteBuffer(env, (*db), sizeof(monetdbe_database)));
        return NULL;
    }
}

JNIEXPORT jstring JNICALL Java_org_monetdb_monetdbe_MonetNative_monetdbe_1open__Ljava_lang_String_2Lorg_monetdb_monetdbe_MonetConnection_2(JNIEnv *env, jclass self, jstring j_url, jobject j_connection)
{
    return open_db(env, j_url, NULL, j_connection);
}

JNIEXPORT jstring JNICALL Java_org_monetdb_monetdbe_MonetNative_monetdbe_1open__Ljava_lang_String_2Lorg_monetdb_monetdbe_MonetConnection_2IIIILjava_lang_String_2(JNIEnv *env, jclass self, jstring j_url, jobject j_connection, jint j_sessiontimeout, jint j_querytimeout, jint j_memorylimit, jint j_nr_threads, jstring j_logfile)
{
    monetdbe_options *opts = set_options(env, j_sessiontimeout, j_querytimeout, j_memorylimit, j_nr_threads,j_logfile);
    return open_db(env, j_url, opts, j_connection);
}

JNIEXPORT jstring JNICALL Java_org_monetdb_monetdbe_MonetNative_monetdbe_1open__Ljava_lang_String_2Lorg_monetdb_monetdbe_MonetConnection_2IIIILjava_lang_String_2ILjava_lang_String_2Ljava_lang_String_2Ljava_lang_String_2Ljava_lang_String_2(JNIEnv *env, jclass self, jstring j_url, jobject j_connection, jint j_sessiontimeout, jint j_querytimeout, jint j_memorylimit, jint j_nr_threads, jstring j_host, jint j_port, jstring j_database, jstring j_user, jstring j_password, jstring j_logfile)
{
    monetdbe_options *opts = set_options(env, j_sessiontimeout, j_querytimeout, j_memorylimit, j_nr_threads,j_logfile);
    set_options_remote(env, opts, j_host, j_port, j_database, j_user, j_password);
    return open_db(env, j_url, opts, j_connection);
}

JNIEXPORT jstring JNICALL Java_org_monetdb_monetdbe_MonetNative_monetdbe_1close(JNIEnv *env, jclass self, jobject j_db)
{
    monetdbe_database db = (*env)->GetDirectBufferAddress(env, j_db);
    int error_code = monetdbe_close(db);
    if (error_code != 0)
    {
        char *error_msg = monetdbe_error(db);
        return (*env)->NewStringUTF(env, (const char *)error_msg);
    }
    else
    {
        return NULL;
    }
}

JNIEXPORT jstring JNICALL Java_org_monetdb_monetdbe_MonetNative_monetdbe_1error(JNIEnv *env, jclass self, jobject j_db)
{
    monetdbe_database db = (*env)->GetDirectBufferAddress(env, j_db);
    char *result = monetdbe_error(db);
    return (*env)->NewStringUTF(env, (const char *)result);
}

JNIEXPORT jstring JNICALL Java_org_monetdb_monetdbe_MonetNative_monetdbe_1set_1autocommit(JNIEnv *env, jclass self, jobject j_db, jint j_auto_commit)
{
    monetdbe_database db = (*env)->GetDirectBufferAddress(env, j_db);
    char *error_msg = monetdbe_set_autocommit(db, j_auto_commit);
    if (error_msg)
    {
        return (*env)->NewStringUTF(env, (const char *)error_msg);
    }
    else
    {
        return NULL;
    }
}

JNIEXPORT jboolean JNICALL Java_org_monetdb_monetdbe_MonetNative_monetdbe_1get_1autocommit(JNIEnv *env, jclass self, jobject j_db)
{
    monetdbe_database db = (*env)->GetDirectBufferAddress(env, j_db);
    int result;
    char *error_msg = monetdbe_get_autocommit(db, &result);
    if (error_msg)
    {
        printf("Error in set_autocommit: %s\n", error_msg);
        return (jboolean)0;
    }
    else
    {
        return (jboolean)result == 1;
    }
}

void returnResult(JNIEnv *env, jobject j_statement, jboolean largeUpdate, monetdbe_result **result, monetdbe_cnt *affected_rows, jint maxrows)
{
    jclass statementClass = (*env)->GetObjectClass(env, j_statement);
    //Query with table result
    if ((*result) && (*result)->ncols > 0)
    {
        jobject resultNative = (*env)->NewDirectByteBuffer(env, (*result), sizeof(monetdbe_result));
        jstring resultSetName = (*env)->NewStringUTF(env, (const char *)(*result)->name);
        jclass resultSetClass = (*env)->FindClass(env, "Lorg/monetdb/monetdbe/MonetResultSet;");
        jmethodID constructor = (*env)->GetMethodID(env, resultSetClass, "<init>", "(Lorg/monetdb/monetdbe/MonetStatement;Ljava/nio/ByteBuffer;IILjava/lang/String;I)V");
        jobject resultSetObject = (*env)->NewObject(env, resultSetClass, constructor, j_statement, resultNative, (*result)->nrows, (*result)->ncols, resultSetName, maxrows);
        free(affected_rows);
        jfieldID resultSetField = (*env)->GetFieldID(env, statementClass, "resultSet", "Lorg/monetdb/monetdbe/MonetResultSet;");
        (*env)->SetObjectField(env, j_statement, resultSetField, resultSetObject);
    }
    //Update query
    else
    {
        if (largeUpdate)
        {
            jfieldID affectRowsField = (*env)->GetFieldID(env, statementClass, "largeUpdateCount", "J");
            (*env)->SetLongField(env, j_statement, affectRowsField, (jlong)(*affected_rows));
        }
        else
        {
            jfieldID affectRowsField = (*env)->GetFieldID(env, statementClass, "updateCount", "I");
            (*env)->SetIntField(env, j_statement, affectRowsField, (jint)(*affected_rows));
        }
        free(affected_rows);
        free(result);
    }
}

JNIEXPORT jstring JNICALL Java_org_monetdb_monetdbe_MonetNative_monetdbe_1query(JNIEnv *env, jclass self, jobject j_db, jstring j_sql, jobject j_statement, jboolean largeUpdate, jint maxrows)
{
    monetdbe_result **result = malloc(sizeof(monetdbe_result *));
    monetdbe_cnt *affected_rows = malloc(sizeof(monetdbe_cnt));
    (*affected_rows) = 0;
    char *sql = (char *)(*env)->GetStringUTFChars(env, j_sql, NULL);
    monetdbe_database db = (*env)->GetDirectBufferAddress(env, j_db);

    char *error_msg = monetdbe_query(db, sql, result, affected_rows);
    (*env)->ReleaseStringUTFChars(env, j_sql, sql);
    if (error_msg)
    {
        return (*env)->NewStringUTF(env, (const char *)error_msg);
    }
    else
    {
        returnResult(env, j_statement, largeUpdate, result, affected_rows, maxrows);
        return NULL;
    }
}

void addColumnVar(JNIEnv *env, jobjectArray j_columns, int index, int type, char *name, jobjectArray j_data)
{
    //Create Java class for result column and set it in the column array
    jstring j_name = (*env)->NewStringUTF(env, (const char *)name);
    jclass j_column = (*env)->FindClass(env, "Lorg/monetdb/monetdbe/MonetColumn;");
    jmethodID constructor = (*env)->GetMethodID(env, j_column, "<init>", "(Ljava/lang/String;I[Ljava/lang/Object;)V");
    jobject j_column_object = (*env)->NewObject(env, j_column, constructor, j_name, type, j_data);
    (*env)->SetObjectArrayElement(env, j_columns, index, j_column_object);
}

//True (1) if time is validated, False (0) if otherwise
int validateTime(monetdbe_data_time t)
{
    if (t.hours >= 0 && t.hours < 24 && t.minutes >= 0 && t.minutes < 60 && t.seconds >= 0 && t.seconds < 60 && t.ms >= 0)
        return 1;
    else
        return 0;
}

//True (1) if date is validated, False (0) if otherwise
int validateDate(monetdbe_data_date d)
{
    if (d.year > 0 && d.month > 0 && d.month <= 12 && d.day > 0 && d.day <= 31)
        return 1;
    else
        return 0;
}

void parseColumnTimestamp(JNIEnv *env, jobjectArray j_columns, int index, monetdbe_column_timestamp *column)
{
    jclass j_timestamp_class = (*env)->FindClass(env, "Ljava/time/LocalDateTime;");
    jmethodID timestamp_constructor = (*env)->GetStaticMethodID(env, j_timestamp_class, "of", "(IIIIIII)Ljava/time/LocalDateTime;");

    jobjectArray j_data = (*env)->NewObjectArray(env, column->count, j_timestamp_class, NULL);
    monetdbe_data_timestamp *timestamps = (monetdbe_data_timestamp *)column->data;

    for (int i = 0; i < column->count; i++)
    {
        //printf("Fetched timestamp: %d-%d-%d\n",(int)timestamps[i].date.year, (int)timestamps[i].date.month, (int)timestamps[i].date.day);
        //fflush(stdout);
        if (column->is_null(&timestamps[i]) == 0 && validateDate(timestamps[i].date) && validateTime(timestamps[i].time))
        {
            monetdbe_data_time time = timestamps[i].time;
            monetdbe_data_date date = timestamps[i].date;
            jobject j_timestamp = (*env)->CallStaticObjectMethod(env, j_timestamp_class, timestamp_constructor, (int)date.year, (int)date.month, (int)date.day, (int)time.hours, (int)time.minutes, (int)time.seconds, ((int)time.ms) * 1000000);
            (*env)->SetObjectArrayElement(env, j_data, i, j_timestamp);
        }
        else
        {
            (*env)->SetObjectArrayElement(env, j_data, i, NULL);
        }
    }

    int type = column->type;
//If int128 is not defined, add 1 to type to "align" the type with versions with int128 defined
#ifndef HAVE_HGE
    type = type + 1;
#endif

    //Inserting LocalDateTime[] in MonetColumn
    addColumnVar(env, j_columns, index, type, column->name, j_data);
}

void parseColumnTime(JNIEnv *env, jobjectArray j_columns, int index, monetdbe_column_time *column)
{
    jclass j_time_class = (*env)->FindClass(env, "Ljava/time/LocalTime;");
    jmethodID time_constructor = (*env)->GetStaticMethodID(env, j_time_class, "of", "(IIII)Ljava/time/LocalTime;");

    jobjectArray j_data = (*env)->NewObjectArray(env, column->count, j_time_class, NULL);
    monetdbe_data_time *times = (monetdbe_data_time *)column->data;

    for (int i = 0; i < column->count; i++)
    {
        if (column->is_null(&times[i]) == 0 && validateTime(times[i]))
        {
            jobject j_time = (*env)->CallStaticObjectMethod(env, j_time_class, time_constructor, (int)times[i].hours, (int)times[i].minutes, (int)times[i].seconds, (int)times[i].ms);
            (*env)->SetObjectArrayElement(env, j_data, i, j_time);
        }
        else
        {
            (*env)->SetObjectArrayElement(env, j_data, i, NULL);
        }
    }
    int type = column->type;
//If int128 is not defined, add 1 to type to "align" the type with versions with int128 defined
#ifndef HAVE_HGE
    type = type + 1;
#endif

    //Inserting LocalTime[] in MonetColumn
    addColumnVar(env, j_columns, index, type, column->name, j_data);
}

void parseColumnDate(JNIEnv *env, jobjectArray j_columns, int index, monetdbe_column_date *column)
{
    jclass j_date_class = (*env)->FindClass(env, "Ljava/time/LocalDate;");
    jmethodID date_constructor = (*env)->GetStaticMethodID(env, j_date_class, "of", "(III)Ljava/time/LocalDate;");

    jobjectArray j_data = (*env)->NewObjectArray(env, column->count, j_date_class, NULL);
    monetdbe_data_date *dates = (monetdbe_data_date *)column->data;

    for (int i = 0; i < column->count; i++)
    {
        if (column->is_null(&dates[i]) == 0 && validateDate(dates[i]))
        {
            jobject j_date = (*env)->CallStaticObjectMethod(env, j_date_class, date_constructor, (int)dates[i].year, (int)dates[i].month, (int)dates[i].day);
            (*env)->SetObjectArrayElement(env, j_data, i, j_date);
        }
        else
        {
            (*env)->SetObjectArrayElement(env, j_data, i, NULL);
        }
    }

    int type = column->type;
//If int128 is not defined, add 1 to type to "align" the type with versions with int128 defined
#ifndef HAVE_HGE
    type = type + 1;
#endif

    //Inserting LocalDate[] in MonetColumn
    addColumnVar(env, j_columns, index, type, column->name, j_data);
}

void parseColumnString(JNIEnv *env, jobjectArray j_columns, int index, monetdbe_column_str *column)
{
    jobjectArray j_data = (*env)->NewObjectArray(env, column->count, (*env)->FindClass(env, "Ljava/lang/String;"), NULL);
    char **strings = (char **)column->data;

    for (int i = 0; i < column->count; i++)
    {
        if (column->is_null(&strings[i]) == 1)
        {
            (*env)->SetObjectArrayElement(env, j_data, i, NULL);
        }
        else
        {
            jobject j_string = (*env)->NewStringUTF(env, (const char *)(strings[i]));
            (*env)->SetObjectArrayElement(env, j_data, i, j_string);
        }
    }

    int type = column->type;
    //If int128 is not defined, add 1 to type to "align" the type with versions with int128 defined
#ifndef HAVE_HGE
    type = type + 1;
#endif

    //Inserting String[] in MonetColumn
    addColumnVar(env, j_columns, index, type, column->name, j_data);
}

void parseColumnBlob(JNIEnv *env, jobjectArray j_columns, int index, monetdbe_column_blob *column)
{
    jobjectArray j_data = (*env)->NewObjectArray(env, column->count, (*env)->FindClass(env, "[B"), NULL);
    monetdbe_data_blob *blob_data = (monetdbe_data_blob *)column->data;

    for (int i = 0; i < column->count; i++)
    {
        if (column->is_null(&blob_data[i]) == 1)
        {
            (*env)->SetObjectArrayElement(env, j_data, i, NULL);
        }
        else
        {
            jbyteArray j_byte_array = (*env)->NewByteArray(env, blob_data[i].size);
            (*env)->SetByteArrayRegion(env, j_byte_array, 0, blob_data[i].size, (jbyte *)blob_data[i].data);
            (*env)->SetObjectArrayElement(env, j_data, i, j_byte_array);
        }
    }

    int type = column->type;
    //If int128 is not defined, add 1 to type to "align" the type with versions with int128 defined
#ifndef HAVE_HGE
    type = type + 1;
#endif

    //Inserting byte[][] in MonetColumn
    addColumnVar(env, j_columns, index, type, column->name, j_data);
}

void addColumnConst(JNIEnv *env, jobjectArray j_columns, void *data, char *name, int type, int row_count, int row_size, int column_number, int precision, int scale, unsigned char *decimalNulls)
{
    jobject j_data = (*env)->NewDirectByteBuffer(env, data, row_count * row_size);
    jstring j_name = (*env)->NewStringUTF(env, (const char *)name);
    jbooleanArray j_decimal_nulls = NULL;

    if (decimalNulls != NULL)
    {
        j_decimal_nulls = (*env)->NewBooleanArray(env, row_count);
        //unsigned char* translates to jboolean*
        (*env)->SetBooleanArrayRegion(env, j_decimal_nulls, 0, row_count, (jboolean *)decimalNulls);
        free(decimalNulls);
    }

    jclass j_column = (*env)->FindClass(env, "Lorg/monetdb/monetdbe/MonetColumn;");
    jmethodID constructor = (*env)->GetMethodID(env, j_column, "<init>", "(Ljava/lang/String;ILjava/nio/ByteBuffer;II[Z)V");

    jobject j_column_object = (*env)->NewObject(env, j_column, constructor, j_name, (jint)type, j_data, precision, scale, j_decimal_nulls);
    (*env)->SetObjectArrayElement(env, j_columns, column_number, j_column_object);
}

//TODO Change return value to string and set jobjectArray through setObjectField?
JNIEXPORT jobjectArray JNICALL Java_org_monetdb_monetdbe_MonetNative_monetdbe_1result_1fetch_1all(JNIEnv *env, jclass self, jobject j_rs, jint nrows, jint ncols)
{
    monetdbe_result *rs = (*env)->GetDirectBufferAddress(env, j_rs);
    monetdbe_column **column = malloc(sizeof(monetdbe_column *));
    jobjectArray j_columns = (*env)->NewObjectArray(env, ncols, (*env)->FindClass(env, "Lorg/monetdb/monetdbe/MonetColumn;"), NULL);

    for (int i = 0; i < ncols; i++)
    {
        char *error_msg = monetdbe_result_fetch(rs, column, i);
        if (error_msg)
        {
            printf("Error in monetdbe_result_fetch: %s\n", error_msg);
            return NULL;
        }
        else
        {
            switch ((*column)->type)
            {
            case monetdbe_bool:
            {
                monetdbe_column_bool *c_bool = (monetdbe_column_bool *)(*column);
                for (int j = 0; j < c_bool->count; j++)
                {
                    if (c_bool->is_null(&c_bool->data[j]) == 1)
                    {
                        c_bool->data[j] = 0;
                    }
                }
                addColumnConst(env, j_columns, c_bool->data, c_bool->name, c_bool->type, c_bool->count, 8, i, c_bool->sql_type.digits, c_bool->sql_type.scale, NULL);
                break;
            }
            case monetdbe_int8_t:
            {
                monetdbe_column_int8_t *c_int8_t = (monetdbe_column_int8_t *)(*column);
                int row_count = c_int8_t->count;
                unsigned char *decimalNulls = NULL;

                for (int j = 0; j < row_count; j++)
                {
                    if (c_int8_t->is_null(&c_int8_t->data[j]) == 1)
                    {
                        c_int8_t->data[j] = 0;

                        //Checking for decimal type nulls
                        if (c_int8_t->scale != 0)
                        {
                            if (decimalNulls == NULL) {
                                decimalNulls = calloc(row_count, sizeof(unsigned char));
                            }
                            decimalNulls[j] = 1;
                        }
                    }
                }
                addColumnConst(env, j_columns, c_int8_t->data, c_int8_t->name, c_int8_t->type, row_count, 8, i, c_int8_t->sql_type.digits, c_int8_t->sql_type.scale, decimalNulls);
                break;
            }
            case monetdbe_int16_t:
            {
                monetdbe_column_int16_t *c_int16_t = (monetdbe_column_int16_t *)(*column);
                int row_count = c_int16_t->count;
                unsigned char *decimalNulls = NULL;

                for (int j = 0; j < row_count; j++)
                {
                    if (c_int16_t->is_null(&c_int16_t->data[j]) == 1)
                    {
                        c_int16_t->data[j] = 0;

                        //Checking for decimal type nulls
                        if (c_int16_t->scale != 0)
                        {
                            if (decimalNulls == NULL) {
                                decimalNulls = calloc(row_count, sizeof(unsigned char));
                            }
                            decimalNulls[j] = 1;
                        }
                    }
                }
                addColumnConst(env, j_columns, c_int16_t->data, c_int16_t->name, c_int16_t->type, row_count, 16, i, c_int16_t->sql_type.digits, c_int16_t->sql_type.scale, decimalNulls);
                break;
            }
            case monetdbe_int32_t:
            {
                monetdbe_column_int32_t *c_int32_t = (monetdbe_column_int32_t *)(*column);
                int row_count = c_int32_t->count;
                unsigned char *decimalNulls = NULL;

                for (int j = 0; j < row_count; j++)
                {
                    if (c_int32_t->is_null(&c_int32_t->data[j]) == 1)
                    {
                        c_int32_t->data[j] = 0;

                        //Checking for decimal type nulls
                        if (c_int32_t->scale != 0)
                        {
                            if (decimalNulls == NULL) {
                                decimalNulls = calloc(row_count, sizeof(unsigned char));
                            }
                            decimalNulls[j] = 1;
                        }
                    }
                }
                addColumnConst(env, j_columns, c_int32_t->data, c_int32_t->name, c_int32_t->type, row_count, 32, i, c_int32_t->sql_type.digits, c_int32_t->sql_type.scale, decimalNulls);
                break;
            }
            case monetdbe_int64_t:
            {
                monetdbe_column_int64_t *c_int64_t = (monetdbe_column_int64_t *)(*column);
                int row_count = c_int64_t->count;
                unsigned char *decimalNulls = NULL;

                for (int j = 0; j < row_count; j++)
                {
                    if (c_int64_t->is_null(&c_int64_t->data[j]) == 1)
                    {
                        c_int64_t->data[j] = 0;

                        //Checking for decimal type nulls
                        if (c_int64_t->scale != 0)
                        {
                            if (decimalNulls == NULL) {
                                decimalNulls = calloc(row_count, sizeof(unsigned char));
                            }
                            decimalNulls[j] = 1;
                        }
                    }
                }
                addColumnConst(env, j_columns, c_int64_t->data, c_int64_t->name, c_int64_t->type, row_count, 64, i, c_int64_t->sql_type.digits, c_int64_t->sql_type.scale, decimalNulls);
                break;
            }
#ifdef HAVE_HGE
            case monetdbe_int128_t:
            {
                monetdbe_column_int128_t *c_int128_t = (monetdbe_column_int128_t *)(*column);
                int row_count = c_int128_t->count;
                unsigned char *decimalNulls = NULL;

                for (int j = 0; j < row_count; j++)
                {
                    if (c_int128_t->is_null(&c_int128_t->data[j]) == 1)
                    {
                        c_int128_t->data[j] = 0;

                        //Checking for decimal type nulls
                        if (c_int128_t->scale != 0)
                        {
                            if (decimalNulls == NULL) {
                                decimalNulls = calloc(row_count, sizeof(unsigned char));
                            }
                            decimalNulls[j] = 1;
                        }
                    }
                }
                addColumnConst(env, j_columns, c_int128_t->data, c_int128_t->name, c_int128_t->type, row_count, 128, i, c_int128_t->sql_type.digits, c_int128_t->sql_type.scale, decimalNulls);
                break;
            }
#endif
            case monetdbe_float:
            {
                monetdbe_column_float *c_float = (monetdbe_column_float *)(*column);
                for (int j = 0; j < c_float->count; j++)
                {
                    if (c_float->is_null(&c_float->data[j]) == 1)
                    {
                        c_float->data[j] = 0;
                    }
                }

                int type = c_float->type;
//If int128 is not defined, add 1 to type to "align" the type with versions with int128 defined
#ifndef HAVE_HGE
                type = type + 1;
#endif
                addColumnConst(env, j_columns, c_float->data, c_float->name, type, c_float->count, 32, i, c_float->sql_type.digits, c_float->sql_type.scale, NULL);
                break;
            }
            case monetdbe_double:
            {
                monetdbe_column_double *c_double = (monetdbe_column_double *)(*column);
                for (int j = 0; j < c_double->count; j++)
                {
                    if (c_double->is_null(&c_double->data[j]) == 1)
                    {
                        c_double->data[j] = 0;
                    }
                }
                int type = c_double->type;
//If int128 is not defined, add 1 to type to "align" the type with versions with int128 defined
#ifndef HAVE_HGE
                type = type + 1;
#endif
                addColumnConst(env, j_columns, c_double->data, c_double->name, type, c_double->count, 64, i, c_double->sql_type.digits, c_double->sql_type.scale, NULL);
                break;
            }
            case monetdbe_str:
            {
                parseColumnString(env, j_columns, i, (monetdbe_column_str *)*column);
                break;
            }
            case monetdbe_blob:
            {
                parseColumnBlob(env, j_columns, i, (monetdbe_column_blob *)*column);
                break;
            }
            case monetdbe_date:
            {
                parseColumnDate(env, j_columns, i, (monetdbe_column_date *)*column);
                break;
            }
            case monetdbe_time:
            {
                parseColumnTime(env, j_columns, i, (monetdbe_column_time *)*column);
                break;
            }
            case monetdbe_timestamp:
            {
                parseColumnTimestamp(env, j_columns, i, (monetdbe_column_timestamp *)*column);
                break;
            }
            default:
                return NULL;
            }
        }
    }
    return j_columns;
}

JNIEXPORT jstring JNICALL Java_org_monetdb_monetdbe_MonetNative_monetdbe_1result_1cleanup(JNIEnv *env, jclass self, jobject j_db, jobject j_rs)
{
    monetdbe_result *rs = (*env)->GetDirectBufferAddress(env, j_rs);
    monetdbe_database db = (*env)->GetDirectBufferAddress(env, j_db);
    char *error_msg = monetdbe_cleanup_result(db, rs);
    if (error_msg)
    {
        return (*env)->NewStringUTF(env, (const char *)error_msg);
    }
    else
    {
        return NULL;
    }
}

void setPreparedStatementInput (JNIEnv *env, jobject j_statement, jclass statementClass, int nInput, monetdbe_result** result, monetdbe_column **column, char **nameData, char **typeData, int *digitsData, int *scaleData) {
    jobjectArray inputMonetdbTypes = (jobjectArray) (*env)->NewObjectArray(env,nInput,(*env)->FindClass(env,"java/lang/String"),NULL);
    jintArray inputDigits = (jintArray) (*env)->NewIntArray(env,nInput);
    jint* inputDigitsArray = malloc(sizeof(jint)*nInput);
    jintArray inputScale = (jintArray) (*env)->NewIntArray(env,nInput);
    jint* inputScaleArray = malloc(sizeof(jint)*nInput);

    //Looping through input columns
    int j = 0;
    for (int i = 0; i < (*column)->count; i++)
    {
        if (nameData[i] == NULL) {
            (*env)->SetObjectArrayElement(env,inputMonetdbTypes,(jsize)j,(*env)->NewStringUTF(env, (const char *)typeData[i]));
            inputDigitsArray[j] = digitsData[i];
            inputScaleArray[j] = scaleData[i];
            j += 1;
        }
    }
    (*env)->SetIntArrayRegion(env,inputDigits,0,nInput,(const jint*)inputDigitsArray);
    (*env)->SetIntArrayRegion(env,inputScale,0,nInput,(const jint*)inputScaleArray);

    jfieldID digitsField = (*env)->GetFieldID(env, statementClass, "digitsInput", "[I");
    jfieldID scaleField = (*env)->GetFieldID(env, statementClass, "scaleInput", "[I");
    (*env)->SetObjectField(env, j_statement, digitsField, inputDigits);
    (*env)->SetObjectField(env, j_statement, scaleField, inputScale);
    free(inputDigitsArray);
    free(inputScaleArray);

    jfieldID paramTypesField = (*env)->GetFieldID(env, statementClass, "paramMonetGDKTypes", "[Ljava/lang/String;");
    (*env)->SetObjectField(env, j_statement, paramTypesField, inputMonetdbTypes);
}

void setPreparedStatementOutput (JNIEnv *env, jobject j_statement, jclass statementClass, int nOutput, monetdbe_result** result, monetdbe_column **column, char **nameData, char **typeData, int *digitsData, int *scaleData) {
    jobjectArray columnNames = (jobjectArray) (*env)->NewObjectArray(env,nOutput,(*env)->FindClass(env,"java/lang/String"),NULL);
    jobjectArray outputMonetdbTypes = (jobjectArray) (*env)->NewObjectArray(env,nOutput,(*env)->FindClass(env,"java/lang/String"),NULL);

    jintArray outputDigits = (jintArray) (*env)->NewIntArray(env,nOutput);
    jint* outputDigitsArray = malloc(sizeof(jint)*nOutput);
    jintArray outputScale = (jintArray) (*env)->NewIntArray(env,nOutput);
    jint* outputScaleArray = malloc(sizeof(jint)*nOutput);

    //Looping through output columns
    int j = 0;
    for (int i = 0; i < (*column)->count; i++)
    {
        if (nameData[i] != NULL) {
            (*env)->SetObjectArrayElement(env,columnNames,(jsize)j,(*env)->NewStringUTF(env, (const char *)nameData[i]));
            (*env)->SetObjectArrayElement(env,outputMonetdbTypes,(jsize)j,(*env)->NewStringUTF(env, (const char *)typeData[i]));
            outputDigitsArray[j] = digitsData[i];
            outputScaleArray[j] = scaleData[i];
            j += 1;
        }
    }
    jfieldID resultTypesField = (*env)->GetFieldID(env, statementClass, "resultMonetGDKTypes", "[Ljava/lang/String;");
    (*env)->SetObjectField(env,j_statement,resultTypesField,(jobjectArray)outputMonetdbTypes);
    jfieldID resultNamesField = (*env)->GetFieldID(env, statementClass, "resultNames", "[Ljava/lang/String;");
    (*env)->SetObjectField(env,j_statement,resultNamesField,(jobjectArray)columnNames);

    (*env)->SetIntArrayRegion(env,outputDigits,0,nOutput,(const jint*)outputDigitsArray);
    (*env)->SetIntArrayRegion(env,outputScale,0,nOutput,(const jint*)outputScaleArray);

    jfieldID digitsField = (*env)->GetFieldID(env, statementClass, "digitsOutput", "[I");
    jfieldID scaleField = (*env)->GetFieldID(env, statementClass, "scaleOutput", "[I");
    (*env)->SetObjectField(env, j_statement, digitsField, outputDigits);
    (*env)->SetObjectField(env, j_statement, scaleField, outputScale);
    free(outputDigitsArray);
    free(outputScaleArray);
}

//TODO Do I need to free every column object after it is retrieved through monetdbe_result_fetch()?
JNIEXPORT jstring JNICALL Java_org_monetdb_monetdbe_MonetNative_monetdbe_1prepare(JNIEnv *env, jclass self, jobject j_db, jstring j_sql, jobject j_statement)
{
    monetdbe_database db = (*env)->GetDirectBufferAddress(env, j_db);
    monetdbe_statement **stmt = malloc(sizeof(monetdbe_statement *));
    char *sql = (char *)(*env)->GetStringUTFChars(env, j_sql, NULL);
    monetdbe_result** result = malloc(sizeof(monetdbe_result *));

    char *error_msg = monetdbe_prepare(db, sql, stmt, result);
    if (error_msg)
    {
        return (*env)->NewStringUTF(env, (const char *)error_msg);
    }
    else
    {
        jclass statementClass = (*env)->GetObjectClass(env, j_statement);
        monetdbe_column **column = malloc(sizeof(monetdbe_column *));
        int nOutput = 0;
        int nInput = 0;

        //Freeing sql query and setting native statement
        (*env)->ReleaseStringUTFChars(env, j_sql, sql);
        jfieldID statementNativeField = (*env)->GetFieldID(env, statementClass, "statementNative", "Ljava/nio/ByteBuffer;");
        (*env)->SetObjectField(env, j_statement, statementNativeField, (*env)->NewDirectByteBuffer(env, (*stmt), sizeof(monetdbe_statement)));

        //Getting column names column (6th column)
        error_msg = monetdbe_result_fetch(*result, column, 5);
        char **nameData = (char **)(*column)->data;
        //Counting number of input and output columns
        for (int i = 0; i < (*column)->count; i++)
        {
            if (nameData[i] != NULL)
                nOutput += 1;
            else
                nInput += 1;
        }
        //Setting number of in/out variables in Java object
        jfieldID nColsField = (*env)->GetFieldID(env, statementClass, "nCols", "I");
        (*env)->SetIntField(env, j_statement, nColsField, (jint)nOutput);
        jfieldID paramsField = (*env)->GetFieldID(env, statementClass, "nParams", "I");
        (*env)->SetIntField(env, j_statement, paramsField, (jint)nInput);

        //Getting MonetDB GDK types column (7th column)
        error_msg = monetdbe_result_fetch(*result, column, 6);
        char **typeData = (char **)(*column)->data;
        //Getting MonetDB digits column (2nd column)
        error_msg = monetdbe_result_fetch(*result, column, 1);
        int *digitsData = (int *)(*column)->data;
        //Getting MonetDB digits column (3rd column)
        error_msg = monetdbe_result_fetch(*result, column, 2);
        int *scaleData = (int *)(*column)->data;

        //Setting input variables
        if (nInput > 0)
            setPreparedStatementInput(env,j_statement,statementClass,nInput,result,column,nameData,typeData,digitsData,scaleData);
        //Setting output variables
        if (nOutput > 0)
            setPreparedStatementOutput(env,j_statement,statementClass,nOutput,result,column,nameData,typeData,digitsData,scaleData);
        free(column);
        free(result);
        return NULL;
    }
}

jstring bind_parsed_data(JNIEnv *env, jobject j_stmt, void *parsed_data, int parameter_nr)
{
    if (parameter_nr < 0)
    {
        return (*env)->NewStringUTF(env, (const char *)"Parameter number is not valid.");
    }
    monetdbe_statement *stmt = (*env)->GetDirectBufferAddress(env, j_stmt);
    char *error_msg = monetdbe_bind(stmt, parsed_data, (int)parameter_nr);
    return (*env)->NewStringUTF(env, (const char *)error_msg);
}

JNIEXPORT jstring JNICALL Java_org_monetdb_monetdbe_MonetNative_monetdbe_1bind_1bool(JNIEnv *env, jclass self, jobject j_stmt, jint parameter_nr, jboolean data)
{
    return bind_parsed_data(env, j_stmt, &data, parameter_nr);
}

JNIEXPORT jstring JNICALL Java_org_monetdb_monetdbe_MonetNative_monetdbe_1bind_1byte(JNIEnv *env, jclass self, jobject j_stmt, jint parameter_nr, jbyte data)
{
    return bind_parsed_data(env, j_stmt, &data, parameter_nr);
}

JNIEXPORT jstring JNICALL Java_org_monetdb_monetdbe_MonetNative_monetdbe_1bind_1short(JNIEnv *env, jclass self, jobject j_stmt, jint parameter_nr, jshort data)
{
    return bind_parsed_data(env, j_stmt, &data, parameter_nr);
}

JNIEXPORT jstring JNICALL Java_org_monetdb_monetdbe_MonetNative_monetdbe_1bind_1int(JNIEnv *env, jclass self, jobject j_stmt, jint parameter_nr, jint data)
{
    return bind_parsed_data(env, j_stmt, &data, parameter_nr);
}

JNIEXPORT jstring JNICALL Java_org_monetdb_monetdbe_MonetNative_monetdbe_1bind_1long(JNIEnv *env, jclass self, jobject j_stmt, jint parameter_nr, jlong data)
{
    return bind_parsed_data(env, j_stmt, &data, parameter_nr);
}

JNIEXPORT jstring JNICALL Java_org_monetdb_monetdbe_MonetNative_monetdbe_1bind_1float(JNIEnv *env, jclass self, jobject j_stmt, jint parameter_nr, jfloat data)
{
    return bind_parsed_data(env, j_stmt, &data, parameter_nr);
}

JNIEXPORT jstring JNICALL Java_org_monetdb_monetdbe_MonetNative_monetdbe_1bind_1double(JNIEnv *env, jclass self, jobject j_stmt, jint parameter_nr, jdouble data)
{
    return bind_parsed_data(env, j_stmt, &data, parameter_nr);
}

JNIEXPORT jstring JNICALL Java_org_monetdb_monetdbe_MonetNative_monetdbe_1bind_1hugeint(JNIEnv *env, jclass self, jobject j_stmt, jint parameter_nr, jobject data)
{
#ifdef HAVE_HGE
    //TODO Parse a BigInteger to int128
    __int128 bind_data = (__int128)1;
    return bind_parsed_data(env, j_stmt, &bind_data, parameter_nr);
#endif
}

JNIEXPORT jstring JNICALL Java_org_monetdb_monetdbe_MonetNative_monetdbe_1bind_1string(JNIEnv *env, jclass self, jobject j_stmt, jint parameter_nr, jstring data)
{
    char *bind_data = (char *)(*env)->GetStringUTFChars(env, data, NULL);
    return bind_parsed_data(env, j_stmt, bind_data, parameter_nr);
}

JNIEXPORT jstring JNICALL Java_org_monetdb_monetdbe_MonetNative_monetdbe_1bind_1null(JNIEnv *env, jclass self, jobject j_db, jint type, jobject j_stmt, jint parameter_nr)
{
    monetdbe_database db = (*env)->GetDirectBufferAddress(env, j_db);
#ifndef HAVE_HGE
    //If int128 is not defined, we need to align the types between Java and C, subtracting one if the type is after int64
    if (type > 4)
        type = type - 1;
#endif

    monetdbe_types null_type = (monetdbe_types)type;
    const void *null_ptr = monetdbe_null(db, null_type);
    return bind_parsed_data(env, j_stmt, (void *)null_ptr, parameter_nr);
}

JNIEXPORT jstring JNICALL Java_org_monetdb_monetdbe_MonetNative_monetdbe_1bind_1blob(JNIEnv *env, jclass self, jobject j_stmt, jint parameter_nr, jbyteArray j_data, jlong size)
{
    jboolean isCopy;
    unsigned char *c_data = (unsigned char *)(*env)->GetByteArrayElements(env, j_data, &isCopy);

    monetdbe_data_blob *bind_data = malloc(sizeof(monetdbe_data_blob));
    bind_data->size = size;
    bind_data->data = (char *)c_data;
    jstring ret_str = bind_parsed_data(env, j_stmt, bind_data, (int)parameter_nr);

    if (isCopy)
    {
        (*env)->ReleaseByteArrayElements(env, j_data, (jbyte *)c_data, JNI_ABORT);
    }
    return ret_str;
}

JNIEXPORT jstring JNICALL Java_org_monetdb_monetdbe_MonetNative_monetdbe_1bind_1date(JNIEnv *env, jclass self, jobject j_stmt, jint parameter_nr, jint year, jint month, jint day)
{
    monetdbe_data_date *date_bind = malloc(sizeof(monetdbe_data_date));
    date_bind->year = (short)year;
    date_bind->month = (unsigned char)month;
    date_bind->day = (unsigned char)day;
    return bind_parsed_data(env, j_stmt, date_bind, (int)parameter_nr);
}

JNIEXPORT jstring JNICALL Java_org_monetdb_monetdbe_MonetNative_monetdbe_1bind_1time(JNIEnv *env, jclass self, jobject j_stmt, jint parameter_nr, jint hours, jint minutes, jint seconds, jint ms)
{
    monetdbe_data_time *time_bind = malloc(sizeof(monetdbe_data_time));
    time_bind->hours = (unsigned char)hours;
    time_bind->minutes = (unsigned char)minutes;
    time_bind->seconds = (unsigned char)seconds;
    time_bind->ms = (unsigned int)ms;
    return bind_parsed_data(env, j_stmt, time_bind, (int)parameter_nr);
}

JNIEXPORT jstring JNICALL Java_org_monetdb_monetdbe_MonetNative_monetdbe_1bind_1timestamp(JNIEnv *env, jclass self, jobject j_stmt, jint parameter_nr, jint year, jint month, jint day, jint hours, jint minutes, jint seconds, jint ms)
{
    monetdbe_data_timestamp *timestamp_bind = malloc(sizeof(monetdbe_data_timestamp));
    (timestamp_bind->date).year = (short)year;
    (timestamp_bind->date).month = (unsigned char)month;
    (timestamp_bind->date).day = (unsigned char)day;
    (timestamp_bind->time).hours = (unsigned char)hours;
    (timestamp_bind->time).minutes = (unsigned char)minutes;
    (timestamp_bind->time).seconds = (unsigned char)seconds;
    (timestamp_bind->time).ms = (unsigned int)ms;
    return bind_parsed_data(env, j_stmt, timestamp_bind, (int)parameter_nr);
}

JNIEXPORT jstring JNICALL Java_org_monetdb_monetdbe_MonetNative_monetdbe_1bind_1decimal(JNIEnv *env, jclass self, jobject j_stmt, jobject j_data, jint type, jint scale, jint parameter_nr)
{
    //TODO Bind decimal
    //What C data type do we map a DECIMAL/NUMERICAL (BigDecimal in java) type to? How do I pass the scale with the bind function
    return NULL;
}

JNIEXPORT jstring JNICALL Java_org_monetdb_monetdbe_MonetNative_monetdbe_1execute(JNIEnv *env, jclass self, jobject j_stmt, jobject j_statement, jboolean largeUpdate, jint maxrows)
{
    monetdbe_statement *stmt = (*env)->GetDirectBufferAddress(env, j_stmt);
    monetdbe_result **result = malloc(sizeof(monetdbe_result *));
    monetdbe_cnt *affected_rows = malloc(sizeof(monetdbe_cnt));

    char *error_msg = monetdbe_execute(stmt, result, affected_rows);
    if (error_msg)
    {
        return (*env)->NewStringUTF(env, (const char *)error_msg);
    }
    else
    {
        returnResult(env, j_statement, largeUpdate, result, affected_rows, maxrows);
        return NULL;
    }
}

JNIEXPORT jstring JNICALL Java_org_monetdb_monetdbe_MonetNative_monetdbe_1cleanup_1statement(JNIEnv *env, jclass self, jobject j_db, jobject j_stmt)
{
    monetdbe_database db = (*env)->GetDirectBufferAddress(env, j_db);
    monetdbe_statement *stmt = (*env)->GetDirectBufferAddress(env, j_stmt);
    char *error_msg = monetdbe_cleanup_statement(db, stmt);
    return (*env)->NewStringUTF(env, (const char *)error_msg);
}