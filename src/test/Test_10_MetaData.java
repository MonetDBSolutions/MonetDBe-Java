package test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.monetdb.monetdbe.MonetResultSet;

public class Test_10_MetaData {

	@Test
	public void simpleInsertAndQueryStatements() {
		Stream.of(Configuration.CONNECTIONS).forEach(x -> simpleInsertAndQueryStatements(x));
	}

	private void simpleInsertAndQueryStatements(String connectionUrl) {
		try (Connection conn = DriverManager.getConnection(connectionUrl, null)) {

			assertNotNull("Could not connect to database with connection string: " + connectionUrl, conn);
			assertFalse(conn.isClosed());
			assertTrue(conn.getAutoCommit());

			try (Statement statement = conn.createStatement();
					ResultSet rs = statement.executeQuery("SELECT 1, 'hey', 200000000000, 912387.3232;")) {

				assertEquals(1, ((MonetResultSet) rs).getRowsNumber());
				assertEquals(4, ((MonetResultSet) rs).getColumnsNumber());

				ResultSetMetaData rsMeta = rs.getMetaData();

				rs.next();
				assertEquals((short) 1, rs.getObject(1));
				assertEquals("%2", rsMeta.getColumnName(1));
				assertEquals(Types.TINYINT, rsMeta.getColumnType(1));
				assertEquals("monetdbe_int8_t", rsMeta.getColumnTypeName(1));
				assertEquals(Short.class.getName(), rsMeta.getColumnClassName(1));
				assertEquals(0, rsMeta.getColumnDisplaySize(1));
			}

			DatabaseMetaData dbMeta = conn.getMetaData();

			assertTrue(StringUtils.isNotBlank(dbMeta.getSQLKeywords()));
			assertTrue(StringUtils.isNotBlank(dbMeta.getNumericFunctions()));
			assertTrue(StringUtils.isNotBlank(dbMeta.getStringFunctions()));
			assertTrue(StringUtils.isNotBlank(dbMeta.getSystemFunctions()));
			assertTrue(StringUtils.isNotBlank(dbMeta.getTimeDateFunctions()));

			try (ResultSet rs = dbMeta.getTableTypes()) {
				assertTrue(((MonetResultSet) rs).getRowsNumber() >= 10);
			}
			try (ResultSet rs = dbMeta.getProcedures(null, null, null)) {
				assertTrue(((MonetResultSet) rs).getRowsNumber() > 70);
				assertEquals(9, ((MonetResultSet) rs).getColumnsNumber());
			}
			try (ResultSet rs = dbMeta.getProcedureColumns(null, null, null, null)) {
				assertTrue(((MonetResultSet) rs).getRowsNumber() >= 10);
				assertEquals(20, ((MonetResultSet) rs).getColumnsNumber());
			}
			try (ResultSet rs = dbMeta.getTables(null, null, null, null)) {
				assertTrue(((MonetResultSet) rs).getRowsNumber() >= 10);
				assertEquals(10, ((MonetResultSet) rs).getColumnsNumber());
			}
			try (ResultSet rs = dbMeta.getSchemas(null, null)) {
				assertEquals(7, ((MonetResultSet) rs).getRowsNumber());
				assertEquals(2, ((MonetResultSet) rs).getColumnsNumber());
			}
			try (ResultSet rs = dbMeta.getColumns(null, null, null, null)) {
				assertTrue(((MonetResultSet) rs).getRowsNumber() >= 10);
				assertEquals(24, ((MonetResultSet) rs).getColumnsNumber());
			}
			try (ResultSet rs = dbMeta.getColumnPrivileges(null, null, null, null)) {
				assertTrue(((MonetResultSet) rs).getRowsNumber() >= 10);
				assertEquals(8, ((MonetResultSet) rs).getColumnsNumber());
			}
			try (ResultSet rs = dbMeta.getTablePrivileges(null, null, null)) {
				assertTrue(((MonetResultSet) rs).getRowsNumber() >= 10);
				assertEquals(7, ((MonetResultSet) rs).getColumnsNumber());
			}
			try (ResultSet rs = dbMeta.getPrimaryKeys(null, null, null)) {
				assertEquals(9, ((MonetResultSet) rs).getRowsNumber());
				assertEquals(6, ((MonetResultSet) rs).getColumnsNumber());
			}
			try (ResultSet rs = dbMeta.getImportedKeys(null, null, null)) {
				assertEquals(0, ((MonetResultSet) rs).getRowsNumber());
				assertEquals(14, ((MonetResultSet) rs).getColumnsNumber());
			}
			try (ResultSet rs = dbMeta.getTypeInfo()) {
				assertTrue(((MonetResultSet) rs).getRowsNumber() >= 10);
				assertEquals(18, ((MonetResultSet) rs).getColumnsNumber());
			}
			try (ResultSet rs = dbMeta.getIndexInfo(null, null, null, false, false)) {
				assertTrue(((MonetResultSet) rs).getRowsNumber() >= 10);
				assertEquals(13, ((MonetResultSet) rs).getColumnsNumber());
			}
			try (ResultSet rs = dbMeta.getFunctions(null, null, null)) {
				assertTrue(((MonetResultSet) rs).getRowsNumber() >= 10);
				assertEquals(6, ((MonetResultSet) rs).getColumnsNumber());
			}
			try (ResultSet rs = dbMeta.getFunctionColumns(null, null, null, null)) {
				assertTrue(((MonetResultSet) rs).getRowsNumber() >= 10);
				assertEquals(17, ((MonetResultSet) rs).getColumnsNumber());
			}
			try (ResultSet rs = dbMeta.getClientInfoProperties()) {
				assertEquals(7, ((MonetResultSet) rs).getRowsNumber());
				assertEquals(4, ((MonetResultSet) rs).getColumnsNumber());
			}

		} catch (SQLException e) {

			fail(e.toString());

		}
	}
}