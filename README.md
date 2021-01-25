# MonetDBe-Java
**Serverless embedded MonetDB in Java**

*TODO: Add description and docs*

# Installing MonetDBe-Java
*TODO: Add dependecies and explain installation better*
## Compiling monetdbe-java.jar
```
$ mkdir build
$ cd build
$ cmake ..
$ make
```
This will output a jar file to the build directory (*monetdbe-java.jar*)

## Running the test class
*TODO: Change this process to mvn(?)*
$MONETDBE_JAVA_PATH is the directory containing the cloned repo.
```
$ javac -cp $MONETDBE_JAVA_PATH/build/monetdbe-java.jar $MONETDBE_JAVA_PATH/src/test/java/TestMonetDBeJava.java
$ java -Djava.library.path=$MONETDBE_JAVA_PATH/build/ -classpath $MONETDBE_JAVA_PATH/build/monetdbe-java.jar:$MONETDBE_JAVA_PATH/src/test/java/ TestMonetDBeJava
```
