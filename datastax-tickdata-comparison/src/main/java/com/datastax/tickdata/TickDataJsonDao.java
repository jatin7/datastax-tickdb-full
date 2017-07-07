package com.datastax.tickdata;

import java.io.IOException;
import java.text.SimpleDateFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;
import com.datastax.driver.core.policies.LoggingRetryPolicy;
import com.datastax.timeseries.model.TimeSeries;
import com.fasterxml.jackson.databind.ObjectMapper;

public class TickDataJsonDao {

	private static Logger logger = LoggerFactory.getLogger(TickDataJsonDao.class);

	private Session session;
	private static String keyspaceName = "datastax_tickdata_json_demo";
	private static String tableNameTick = keyspaceName + ".tick_data";

	private static final String INSERT_INTO_TICK = "Insert into " + tableNameTick + " (symbol,values) values (?, ?);";
	private static final String SELECT_FROM_TICK = "Select symbol, values from " + tableNameTick + " where symbol = ?";

	private PreparedStatement insertStmtTick;
	private PreparedStatement selectStmtTick;
	private ObjectMapper objectMapper = new ObjectMapper();

	private SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.zzz");

	public TickDataJsonDao(String[] contactPoints) {

		Cluster cluster = Cluster.builder()
				.withRetryPolicy(new LoggingRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE))
				.addContactPoints(contactPoints).build();

		this.session = cluster.connect();

		this.insertStmtTick = session.prepare(INSERT_INTO_TICK);
		this.insertStmtTick.setConsistencyLevel(ConsistencyLevel.ONE);
		this.selectStmtTick = session.prepare(SELECT_FROM_TICK);
		this.selectStmtTick.setConsistencyLevel(ConsistencyLevel.ONE);
	}

	public TimeSeries getTimeSeries(String symbol){
		
		BoundStatement boundStmt = new BoundStatement(this.selectStmtTick);
		boundStmt.setString(0, symbol);
				
		ResultSet resultSet = session.execute(boundStmt);
				
		if (resultSet.isExhausted()){
			logger.debug("No results found for symbol : " + symbol);
		}else{
			
			Row row = resultSet.one();		
			String json = row.getString(1);
			
			try {
				return objectMapper.readValue(json, TimeSeries.class);
			} catch (IOException e) {
				
				e.printStackTrace();
			}
		}
		return new TimeSeries(symbol, null, null);
		
	}

	public void insertTimeSeries(TimeSeries timeSeries) throws Exception {
		logger.info("Writing Json : " + timeSeries.getSymbol() + " - " + timeSeries.getDates().length);

		BoundStatement boundStmt = new BoundStatement(this.insertStmtTick);
		session.execute(boundStmt.bind(timeSeries.getSymbol(), objectMapper.writeValueAsString(timeSeries)));
		return;
	}

	private String fillNumber(int num) {
		return num < 10 ? "0" + num : "" + num;
	}
}
