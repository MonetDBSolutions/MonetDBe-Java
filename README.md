# MonetDBe-Java
**Serverless embedded MonetDB in Java**

*TODO: Add description and docs*

# Installing MonetDBe-Java
*TODO: Add dependecies and explain installation better*

```
git clone https://github.com/MonetDBSolutions/MonetDBe-Java MonetDBe-Java
cd MonetDBe-Java
```
## Compiling monetdbe-java.jar
```
$ mkdir build && cd build
$ cmake ..
$ make
```
This will output a jar file to the build directory (*monetdbe-java.jar*)

## Running the example class
$MONETDBE_JAVA_PATH is the directory containing the cloned repo.
```
$ javac -cp $MONETDBE_JAVA_PATH/build/monetdbe-java.jar $MONETDBE_JAVA_PATH/example/TestMonetDBeJava.java
$ java -Djava.library.path=$MONETDBE_JAVA_PATH/build/ -classpath $MONETDBE_JAVA_PATH/build/monetdbe-java.jar:$MONETDBE_JAVA_PATH/example/ TestMonetDBeJava
```
