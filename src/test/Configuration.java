package test;

public class Configuration {

	protected static final String MEMORY_CONNECTION = "jdbc:monetdb:memory:";
	protected static final String LOCAL_CONNECTION = "jdbc:monetdb:file:./testdata/localdb";
	protected static final String PROXY_CONNECTION = "mapi:monetdb://localhost:50000/test";

	protected static final String AUTOCOMMIT_FALSE_PARM = "?autocommit=false";

	protected static final String[] CONNECTIONS = { MEMORY_CONNECTION, LOCAL_CONNECTION };

}
