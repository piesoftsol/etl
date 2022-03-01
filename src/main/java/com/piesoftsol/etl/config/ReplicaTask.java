package com.piesoftsol.etl.config;

import org.springframework.jdbc.core.JdbcTemplate;

import com.piesoftsol.etl.dao.CallProcedure;
import com.piesoftsol.etl.services.PGManager;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

final public class ReplicaTask implements Callable<Integer> {

    private int taskId;
    private String taskName;
    private int _chunkSize;
    private JdbcTemplate _jdbcTemplate;
    String tableName = "tb_dummy_data";
    List<String> columns = new ArrayList<>();


    public ReplicaTask(int id, JdbcTemplate jdbcTemplate, int chunkSize) {
        this.taskId = id;
        this._jdbcTemplate = jdbcTemplate;
        this._chunkSize = chunkSize;
    }

    @Override
    public Integer call() throws Exception {

        //System.out.println("Task ID :" + this.taskId + " performed by " + Thread.currentThread().getName());
        this.taskName = "TaskId-"+this.taskId;

        Thread.currentThread().setName(taskName);

        System.out.println("Starting " + Thread.currentThread().getName());
        
        columns.add("id");
        columns.add("name");
        columns.add("age");
        PGManager pgManager = new PGManager(this._jdbcTemplate);
        ResultSet resultSet = pgManager.readTable(tableName, columns, taskId, _chunkSize);
        while(resultSet.next()) {
        	CallProcedure callProcedure = new CallProcedure(_jdbcTemplate);
        	callProcedure.callProc(resultSet.getInt("id"));
        	System.out.println("AAA " + resultSet.getInt("age"));
        }
        System.out.println("Ending " + Thread.currentThread().getName() + " == ");
        return this.taskId;
    }
}
