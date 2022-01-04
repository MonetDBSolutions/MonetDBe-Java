package test;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

//Oct2020 fails Test 21 (Timeout not reached), Test 22 (BigDecimal isn't scaled), Test 18 (Oct2020 does not support the new way of multithreaded connection?), plus some tests fail with "unable to drop table x"
//Jul2021 passes all the tests except Test 22 (BigDecimal isn't scaled), but Tests 19 and 20 have to be modified

//Jan2022 passes all the tests (memory and file databases)
//Jan2022 remote connection errors:
//	04 -> update count error
//	05 -> update count error
//	06 -> update count error
//	07 -> update count error
//	08 -> update count error
// 	12 -> batchPreparedQuery parameter 2 not bound to value -> monetdbe thinks there is an extra timestamp parameter to bind
//	16 -> update count error
//	17 -> update count error
//	18 -> CORRECT: crashes if run through junit framework, but the multithreaded code works fine (RemoteConnectionTests example)
//	19 -> PreparedStatement getParameterCount is wrong -> monetdbe thinks there is an extra timestamp parameter to bind
//	21 -> MALException:mal.interpreter:HYT00!Query aborted due to session timeout // Timeout: 2000 	Execution time: 3
// 	22 -> scale is not being set on DECIMAL; update count error
@RunWith(Suite.class)
@SuiteClasses({ Test_01_OpenAndCloseConnection.class, Test_02_CloseConnectionTwice.class, Test_03_AutoCommit.class,
		Test_04_SimpleInsertAndQueryStatements.class, Test_05_BasicInsertAndQueryStatements.class,
		Test_06_ComplexInsertAndQueryStatements.class, Test_07_SimplePreparedStatements.class,
		Test_08_ComplexPreparedStatements.class, Test_09_LoadAndQueryTaxi.class, Test_10_MetaData.class,
		Test_11_ConcurrentConnections.class, Test_12_BatchesAndJoinsMovies.class, Test_13_Schema.class,
		//Test_14_MultipleResultSet.class,
		Test_15_Transactions.class, Test_16_MixedOrderStatements.class, Test_17_QueryInThread.class,
		Test_18_Multithreaded_Connection.class, Test_19_ParameterMetadata.class, Test_20_PreparedResultMetadata.class,
		Test_21_ConnectionOptions.class, Test_22_GetObject.class})
public class AllTests {

	protected static final String MEMORY_CONNECTION = "jdbc:monetdb:memory:";
	//Maven runs tests from the java directory, testdata folder should be on the root of the repo
	protected static final String LOCAL_CONNECTION = "jdbc:monetdb:file:" + System.getProperty("user.dir") + "/../testdata/localdb";
	//protected static final String LOCAL_CONNECTION = "jdbc:monetdb:file:./testdata/localdb";
	protected static final String PROXY_CONNECTION = "mapi:monetdb://localhost:50000/test";
	
	protected static final String[] CONNECTIONS = {  MEMORY_CONNECTION, LOCAL_CONNECTION };

	protected static final String AUTOCOMMIT_FALSE_PARM = "?autocommit=false";

	//Maven runs tests from the java directory, testdata folder should be on the root of the repo
	protected static final String TAXI_CSV = System.getProperty("user.dir") + "/../testdata/taxi/yellow_tripdata_2016-01.csv";
	//protected static final String TAXI_CSV = "./testdata/taxi/yellow_tripdata_2016-01.csv";

}
