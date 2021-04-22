package test;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({ Test_01_OpenAndCloseConnection.class, Test_02_CloseConnectionTwice.class, Test_03_AutoCommit.class,
		Test_04_SimpleInsertAndQueryStatements.class, Test_05_BasicInsertAndQueryStatements.class,
		Test_06_ComplexInsertAndQueryStatements.class, Test_07_SimplePreparedStatements.class,
		Test_08_ComplexPreparedStatements.class, Test_09_LoadAndQueryTaxi.class, Test_10_MetaData.class,
		Test_11_ConcurrentConnections.class, Test_12_BatchesAndJoinsMovies.class,
		Test_13_Schema.class })
public class AllTests {

}
