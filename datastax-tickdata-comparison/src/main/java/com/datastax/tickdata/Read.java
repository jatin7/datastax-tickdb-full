package com.datastax.tickdata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.demo.utils.PropertyHelper;
import com.datastax.demo.utils.Timer;
import com.datastax.timeseries.model.TimeSeries;

public class Read {
	private static Logger logger = LoggerFactory.getLogger(Read.class);


	public Read() {

		String contactPointsStr = PropertyHelper.getProperty("contactPoints", "localhost");
		String symbol = PropertyHelper.getProperty("symbol", "NASDAQ-AAPL-2017-06-16");
		
		TickDataDao dao = new TickDataDao(contactPointsStr.split(","));
		TickDataJsonDao jsonDao = new TickDataJsonDao(contactPointsStr.split(","));
		TickDataBinaryDao binaryDao = new TickDataBinaryDao(contactPointsStr.split(","));
		
		Timer timer = new Timer();		
		TimeSeries timeSeries = binaryDao.getTimeSeries(symbol);			
		timer.end();
		logger.info("Data read took with binary " + timer.getTimeTakenMillis() + " ms. Total Points " + timeSeries.getDates().length);
		
		timer = new Timer();		
		TimeSeries tickData = dao.getTimeSeries(symbol);			
		timer.end();
		logger.info("Data read took with tick " + timer.getTimeTakenMillis() + " ms. Total Points " + tickData.getDates().length);

		timer = new Timer();
		TimeSeries jsonData = jsonDao.getTimeSeries(symbol);		
		timer.end();
		logger.info("Data read took with json " + timer.getTimeTakenMillis() + " ms. Total Points " + jsonData.getDates().length);		
		
		System.exit(0);
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		new Read();
	}
}
