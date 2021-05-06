package test;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({ Test_01_OpenAndCloseConnection.class, Test_02_CloseConnectionTwice.class, Test_03_AutoCommit.class,
		Test_04_SimpleInsertAndQueryStatements.class, Test_05_BasicInsertAndQueryStatements.class,
		Test_06_ComplexInsertAndQueryStatements.class, Test_07_SimplePreparedStatements.class,
		Test_08_ComplexPreparedStatements.class, Test_09_LoadAndQueryTaxi.class, Test_10_MetaData.class,
		Test_11_ConcurrentConnections.class, Test_12_BatchesAndJoinsMovies.class, Test_13_Schema.class,
		//Test_14_MultipleResultSet.class,
		Test_15_Transactions.class, Test_16_MixedOrderStatements.class, Test_17_QueryInThread.class })
public class AllTests {

	protected static final String MEMORY_CONNECTION = "jdbc:monetdb:memory:";
	//Maven runs tests from the java directory, testdata folder should be on the root of the repo
	protected static final String LOCAL_CONNECTION = "jdbc:monetdb:file:" + System.getProperty("user.dir") + "/../testdata/localdb";
	//protected static final String LOCAL_CONNECTION = "jdbc:monetdb:file:./testdata/localdb";
	protected static final String PROXY_CONNECTION = "mapi:monetdb://localhost:50000/test";
	
	protected static final String[] CONNECTIONS = { MEMORY_CONNECTION, LOCAL_CONNECTION };

	protected static final String AUTOCOMMIT_FALSE_PARM = "?autocommit=false";

	//Maven runs tests from the java directory, testdata folder should be on the root of the repo
	protected static final String TAXI_CSV = System.getProperty("user.dir") + "/../testdata/taxi/yellow_tripdata_2016-01.csv";
	//protected static final String TAXI_CSV = "./testdata/taxi/yellow_tripdata_2016-01.csv";

}
