package test;

public class RunTestsApp {

	public static void main(String[] args) {
		ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
		
		new Test_01_OpenAndCloseConnection().openAndCloseConnection();
		new Test_02_CloseConnectionTwice().closeConnectionTwice();
		new Test_03_AutoCommit().autoCommit();
		new Test_04_SimpleInsertAndQueryStatements().simpleInsertAndQueryStatements();
		new Test_05_BasicInsertAndQueryStatements().basicInsertAndQueryStatements();
		new Test_06_ComplexInsertAndQueryStatements().complexInsertAndQueryStatements();
		new Test_07_SimplePreparedStatements().simplePreparedStatements();
		new Test_08_ComplexPreparedStatements().complexPreparedStatements();
		new Test_09_LoadAndQueryTaxi().loadAndQueryTaxi();
		new Test_10_MetaData().metaData();
		new Test_11_ConcurrentConnections().concurrentConnections();
		new Test_12_BatchesAndJoinsMovies().batchesAndJoinsMovies();
		new Test_13_Schema().schema();
		// new Test_14_MultipleResultSet().multipleResultSet();
		new Test_15_Transactions().transactions();
		new Test_16_MixedOrderStatements().mixedOrderStatements();
		new Test_17_QueryInThread().queryInThread();
		new Test_18_Multithreaded_Connection().multithreadedConnection();
		new Test_19_ParameterMetadata().parameterMetadata();
		new Test_20_PreparedResultMetadata().preparedResultMetadata();
	}

}
