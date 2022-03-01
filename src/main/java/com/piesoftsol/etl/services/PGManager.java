package com.piesoftsol.etl.services;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.springframework.jdbc.core.JdbcTemplate;

public class PGManager {

	private Statement lastStatement;
	private JdbcTemplate _jdbcTemplate;

	public PGManager(JdbcTemplate jdbcTemplate) {
		_jdbcTemplate = jdbcTemplate;
	}

	/**
	 * Executes an arbitrary SQL statement.
	 *
	 * @param stmt      The SQL statement to execute
	 * @param fetchSize Overrides default or parameterized fetch size
	 * @return A ResultSet encapsulating the results or null on error
	 */
	protected ResultSet execute(String stmt, Integer fetchSize, Object... args) throws SQLException {
		// Release any previously-open statement.
		release();

		PreparedStatement statement = this._jdbcTemplate.getDataSource().getConnection().prepareStatement(stmt,
				ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

		if (fetchSize != null) {
			System.out.println(Thread.currentThread().getName() + ": Using fetchSize for next query: " + fetchSize);
			statement.setFetchSize(fetchSize);
		}
		this.lastStatement = statement;
		if (null != args) {
			for (int i = 0; i < args.length; i++) {
				statement.setObject(i + 1, args[i]);
			}
		}

		System.out.println(Thread.currentThread().getName() + ": Executing SQL statement: " + stmt);

		StringBuilder sb = new StringBuilder();
		for (Object o : args) {
			sb.append(o.toString()).append(", ");
		}
		
		System.out.println(Thread.currentThread().getName() + ": With args: " + sb.toString());

		return statement.executeQuery();
	}

	public void release() {
		if (null != this.lastStatement) {
			try {
				this.lastStatement.close();
			} catch (SQLException e) {
				System.out.println("Exception closing executed Statement: " + e);
			}

			this.lastStatement = null;
		}
	}

	public ResultSet readTable(String tableName, List<String> columns, int nThread, int chunkSize) throws SQLException {

		String allColumns = StringUtils.join(columns, ", ");
		long offset = nThread * chunkSize;
		String sqlCmd;

		sqlCmd = "SELECT " + allColumns + " FROM " + tableName;

		sqlCmd = sqlCmd + " WHERE flag = 'N'";

		sqlCmd = sqlCmd + " OFFSET ? ";

		String limit = " LIMIT ?";

		sqlCmd = sqlCmd + limit;
		return execute(sqlCmd, offset, chunkSize);

	}

	protected ResultSet execute(String stmt, Object... args) throws SQLException {
		return execute(stmt, 100, args);
	}

}
