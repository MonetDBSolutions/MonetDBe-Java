#Revise
cmake_minimum_required(VERSION 3.10)

#Set CMAKE library find variables
set(CMAKE_FIND_LIBRARY_PREFIXES "lib")
set(CMAKE_FIND_LIBRARY_SUFFIXES ".so" ".lib" ".a" ".dylib")

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
set_target_properties(monetdbe-lowlevel PROPERTIES SUFFIX ".so")
set_target_properties(monetdbe-lowlevel PROPERTIES PREFIX "lib")

add_custom_command(OUTPUT monetdbe-jdbc
                   DEPENDS monetdbe-lowlevel monetdbe-java
                   COMMAND ${Java_JAR_EXECUTABLE}
                           uf
                           monetdbe-java.jar
                           -C
                           $<TARGET_FILE_DIR:monetdbe-lowlevel>
                           $<TARGET_FILE_NAME:monetdbe-lowlevel>)

add_custom_target(jdbc ALL DEPENDS monetdbe-jdbc)