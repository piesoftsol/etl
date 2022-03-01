package com.piesoftsol.etl.dao;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.jdbc.core.CallableStatementCreator;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.SqlOutParameter;
import org.springframework.jdbc.core.SqlParameter;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.simple.SimpleJdbcCall;

public class CallProcedure {

	private JdbcTemplate _jdbcTemplate;

	public CallProcedure(JdbcTemplate jdbcTemplate) {

		this._jdbcTemplate = jdbcTemplate;

	}

	@SuppressWarnings("unchecked")
	public void callProc(int id) {
		
		
		 List paramList = new ArrayList<SqlParameter>();
		    paramList.add(new SqlParameter(Types.VARCHAR));
		    paramList.add(new SqlOutParameter("retval", Types.VARCHAR));

		    Map<String, Object> resultMap = _jdbcTemplate.call(new CallableStatementCreator() {

		    @Override
		    public CallableStatement createCallableStatement(Connection connection)
		    throws SQLException {

		    CallableStatement callableStatement = connection.prepareCall("{call DUMMYPROCEDURE(?, ?)}");
		    callableStatement.setString(1, "id");
		            callableStatement.registerOutParameter(2, Types.VARCHAR);
		    return callableStatement;

		    }
		    }, paramList);
		

	}

}
