#Revise
cmake_minimum_required(VERSION 3.10)

#Library dependencies
find_package(Java 1.8)
find_package(JNI)

#Project
include(UseJava)
project(MonetDBe-Java)

#Compile monetdbe-java without linked library, generate headers
set(CMAKE_JAVA_COMPILE_FLAGS -source 1.8 -target 1.8)
add_jar(monetdbe-java
        src/main/Java/nl/cwi/monetdb/monetdbe/*.java
        GENERATE_NATIVE_HEADERS
        monetdbe-native)

#Link libraries (Java generated header, JNI wrapper and C library)
add_library(monetdbe-lowlevel SHARED src/jni/monetdbe._lowlevel.c)
target_link_libraries(monetdbe-lowlevel monetdbe-native monetdbe)

add_custom_command(OUTPUT monetdbe-jdbc
                   DEPENDS monetdbe-lowlevel monetdbe-java
                   COMMAND ${Java_JAR_EXECUTABLE}
                           uf
                           monetdbe-java.jar
                           -C
                           $<TARGET_FILE_DIR:monetdbe-lowlevel>
                           $<TARGET_FILE_NAME:monetdbe-lowlevel>)

add_custom_target(jdbc ALL DEPENDS monetdbe-jdbc)

if (FALSE)
    file(GLOB JAVA_SRC_FILES src/main/java/org/duckdb/*.java)
    file(GLOB JAVA_TEST_FILES src/test/java/org/duckdb/test/*.java)

    set(OS_NAME "unknown")
    if (APPLE)
    	set(OS_NAME "osx")
    endif()
    if (WIN32)
    	set(OS_NAME "windows")
    endif()
    if (UNIX AND NOT APPLE)
    	set(OS_NAME "linux") # sorry BSD
    endif()

    set(JVM_BITNESS "amd64")

    include_directories(../../extension/parquet/include)

    add_library(duckdb_java SHARED src/jni/duckdb_java.cpp)
    target_compile_options(duckdb_java PRIVATE -fexceptions)
    target_link_libraries(duckdb_java duckdb-native duckdb_static parquet_extension)
    string(JOIN "_" LIB_SUFFIX ".so" ${OS_NAME} ${JVM_BITNESS})
    set_target_properties(duckdb_java PROPERTIES SUFFIX ${LIB_SUFFIX})
    set_target_properties(duckdb_java PROPERTIES PREFIX "lib")

    add_custom_command(OUTPUT dummy_jdbc_target
                       DEPENDS duckdb_java duckdb_jdbc
                       COMMAND ${Java_JAR_EXECUTABLE}
                               uf
                               duckdb_jdbc.jar
                               -C
                               $<TARGET_FILE_DIR:duckdb_java>
                               $<TARGET_FILE_NAME:duckdb_java>)

    add_custom_target(jdbc ALL DEPENDS dummy_jdbc_target)
endif




