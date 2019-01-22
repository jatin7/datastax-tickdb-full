package com.datastax.tickdata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.demo.utils.FileUtils;
import com.datastax.demo.utils.PropertyHelper;
import com.datastax.demo.utils.Timer;
import com.datastax.timeseries.model.TimeSeries;

public class Read {
	private static Logger logger = LoggerFactory.getLogger(Read.class);


	public Read() {

		String contactPointsStr = PropertyHelper.getProperty("contactPoints", "localhost");
		String symbol = PropertyHelper.getProperty("symbol", "NASDAQ-AAPL-2019-01-01");
		
		TickDataDao dao = new TickDataDao(contactPointsStr.split(","));
		TickDataJsonDao jsonDao = new TickDataJsonDao(contactPointsStr.split(","));
		TickDataBinaryDao binaryDao = new TickDataBinaryDao(contactPointsStr.split(","));
		
		Timer timer = new Timer();		
		TimeSeries timeSeries = binaryDao.getTimeSeries(symbol);			
		timer.end();
		logger.info("Data read took with binary " + timer.getTimeTakenMillis() + " ms. Total Points " + timeSeries.getDates().length + ". Object size : " + FileUtils.getObjectSize(timeSeries));
		
		timer = new Timer();		
		TimeSeries tickData = dao.getTimeSeries(symbol);			
		timer.end();
		logger.info("Data read took with tick " + timer.getTimeTakenMillis() + " ms. Total Points " + tickData.getDates().length + ". Object size : " + FileUtils.getObjectSize(tickData));

		timer = new Timer();
		TimeSeries jsonData = jsonDao.getTimeSeries(symbol);
		
		
		timer.end();
		//logger.info("Data read took with json " + timer.getTimeTakenMillis() + " ms. Total Points " + jsonData.getDates().length + ". Object size : " + FileUtils.getObjectSize(jsonData));		
		
		System.exit(0);
	}
	
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		new Read();
	}
}
