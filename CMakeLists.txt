#Revise
cmake_minimum_required(VERSION 3.10)

#Set CMAKE library find variables
set(CMAKE_FIND_LIBRARY_PREFIXES "lib")
set(CMAKE_FIND_LIBRARY_SUFFIXES ".so" ".lib" ".a" ".dylib")

#Library dependencies
find_package(Java 1.8 REQUIRED)
set(JAVA_AWT_LIBRARY NotNeeded)
find_package(JNI REQUIRED)

#Project
include(UseJava)
project(MonetDBe-Java)

#Compile monetdbe-java without linked library, generate headers
set(CMAKE_JAVA_COMPILE_FLAGS -source 1.8 -target 1.8)
file(GLOB JAVA_SRC_FILES src/main/java/nl/cwi/monetdb/monetdbe/*.java)

add_jar(monetdbe-java
        ${JAVA_SRC_FILES}
        META-INF/services/java.sql.Driver
        GENERATE_NATIVE_HEADERS
        monetdbe-native)

#Link libraries (Java generated header, JNI wrapper and C library)
#link_directories("/home/bernardo/MonetDB-Jun2020/build/tools/monetdbe")
#find_package(MonetDB CONFIG REQUIRED)

add_library(monetdbe-lowlevel SHARED src/jni/monetdbe._lowlevel.c)

include_directories(${PREFIX}/lib64)
include_directories(${PREFIX}/include/monetdb/)
link_directories(${PREFIX}/lib64)
target_compile_options(monetdbe-lowlevel PRIVATE -fexceptions)
set_target_properties(monetdbe-lowlevel PROPERTIES SUFFIX ".so")
set_target_properties(monetdbe-lowlevel PROPERTIES PREFIX "lib")
target_link_libraries(monetdbe-lowlevel monetdbe-native ${PREFIX}/lib64/libmonetdbe.so)

add_custom_command(OUTPUT monetdbe-jdbc
                   DEPENDS monetdbe-lowlevel monetdbe-java
                   COMMAND ${Java_JAR_EXECUTABLE}
                           uf
                           monetdbe-java.jar
                           -C
                           $<TARGET_FILE_DIR:monetdbe-lowlevel>
                           $<TARGET_FILE_NAME:monetdbe-lowlevel>)

add_custom_target(jdbc ALL DEPENDS monetdbe-jdbc)
