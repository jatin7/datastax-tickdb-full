package com.datastax.timeseries.model;

import org.joda.time.DateTime;

public class CandleStick {

	private double high;
	private double low;
	private double open;
	private double close;
	private double avg;
	private double sum;
	private long startTime;
	
	public CandleStick(double high, double low, double open, double close, double avg, double sum) {
		super();
		this.high = high;
		this.low = low;
		this.open = open;
		this.close = close;
		this.avg = avg;
		this.sum = sum;
	}
	public double getHigh() {
		return high;
	}
	public double getLow() {
		return low;
	}
	public double getOpen() {
		return open;
	}
	public double getClose() {
		return close;
	}
	
	public String getStartTime(){
		return new DateTime(startTime).toString();
	}
	
	public void setStartTime(long startTime) {
		this.startTime =startTime;		
	}

	public double getAvg() {
		return avg;
	}
	public void setAvg(double avg) {
		this.avg = avg;
	}
	public double getSum() {
		return sum;
	}
	public void setSum(double sum) {
		this.sum = sum;
	}
	@Override
	public String toString() {
		return "CandleStick [high=" + high + ", low=" + low + ", open=" + open + ", close=" + close + ", avg=" + avg
				+ ", sum=" + sum + ", startTime=" + startTime + "]";
	}
}
