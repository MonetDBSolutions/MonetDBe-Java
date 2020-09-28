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
link_directories("/home/bernardo/MonetDB-Jun2020/build/tools/monetdbe")
add_library(monetdbe-lowlevel SHARED src/jni/monetdbe._lowlevel.c)
#find_package(MonetDB CONFIG REQUIRED)
target_include_directories(monetdbe-lowlevel PRIVATE "/home/bernardo/MonetDB-Jun2020/dist/include/monetdb")
target_include_directories(monetdbe-lowlevel PRIVATE "/home/bernardo/MonetDB-Jun2020/MonetDB/tools/monetdbe")
target_include_directories(monetdbe-lowlevel PRIVATE "/home/bernardo/MonetDB-Jun2020/MonetDB/monetdb5/mal")
target_include_directories(monetdbe-lowlevel PRIVATE "/home/bernardo/MonetDB-Jun2020/MonetDB/monetdb5/modules/atoms")
target_include_directories(monetdbe-lowlevel PRIVATE "/home/bernardo/MonetDB-Jun2020/MonetDB/monetdb5/modules/mal")
target_include_directories(monetdbe-lowlevel PRIVATE "/home/bernardo/MonetDB-Jun2020/MonetDB/sql/server/")
target_include_directories(monetdbe-lowlevel PRIVATE "/home/bernardo/MonetDB-Jun2020/MonetDB/sql/include/")
target_include_directories(monetdbe-lowlevel PRIVATE "/home/bernardo/MonetDB-Jun2020/MonetDB/sql/common/")
target_include_directories(monetdbe-lowlevel PRIVATE "/home/bernardo/MonetDB-Jun2020/MonetDB/sql/storage/")
target_include_directories(monetdbe-lowlevel PRIVATE "/home/bernardo/MonetDB-Jun2020/MonetDB/sql/backends/monet5")
target_include_directories(monetdbe-lowlevel PRIVATE "/home/bernardo/MonetDB-Jun2020/MonetDB/gdk")
target_link_libraries(monetdbe-lowlevel monetdbe-native monetdbe)
set_target_properties(monetdbe-lowlevel PROPERTIES SUFFIX ".so")
set_target_properties(monetdbe-lowlevel PROPERTIES PREFIX "lib")

add_custom_command(OUTPUT monetdbe-jdbc
                   DEPENDS monetdbe-lowlevel monetdbe-java monetdbe-java-test
                   COMMAND ${Java_JAR_EXECUTABLE}
                           uf
                           monetdbe-java.jar
                           -C
                           $<TARGET_FILE_DIR:monetdbe-lowlevel>
                           $<TARGET_FILE_NAME:monetdbe-lowlevel>)

add_custom_target(jdbc ALL DEPENDS monetdbe-jdbc)
