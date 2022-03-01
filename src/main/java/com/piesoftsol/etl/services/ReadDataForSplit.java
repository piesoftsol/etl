package com.piesoftsol.etl.services;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import com.piesoftsol.etl.config.ReplicaTask;

@Service
public class ReadDataForSplit {

	@Autowired
	private JdbcTemplate jdbcTemplate;
	
	ExecutorService replicaTasksService = null;
	
	private int jobCount = 1;
	private int chunkSize = 1;
	
	@Scheduled(cron = "*/10 * * * * *")
	public void readDataFromMasterTable() throws InterruptedException, ExecutionException, SQLException {
		
		jobCount = jdbcTemplate.queryForObject("select count(1) from tb_master_data", Integer.class);
		System.out.println(jobCount);
		
		List<ReplicaTask> replicaTasks = new ArrayList<>();
		
		preSourceTasks();
		
		for (int i = 0; i < jobCount; i++) {
            replicaTasks.add(new ReplicaTask(i, this.jdbcTemplate, this.chunkSize));
        }
		
		
		replicaTasksService = Executors.newFixedThreadPool(jobCount);
		
		List<Future<Integer>> futures = replicaTasksService.invokeAll(replicaTasks);
		
		for (Future<Integer> future : futures) {
            // catch tasks exceptions
            future.get();
        }
		
        replicaTasksService.shutdown();
		
	}
	
	public void preSourceTasks() throws SQLException {
		
		
		if(this.jobCount != 1) {
			try {
				String sql = "SELECT " +
                        " abs(count(*) / " + jobCount + ") chunk_size" +
                        " FROM tb_master_data where flag = 'N' ";
				
				chunkSize = this.jdbcTemplate.queryForObject(sql, Integer.class); 
				
				System.out.println(chunkSize);
				
			}catch (Exception e) {
				
			}
		}

    }
	
}
