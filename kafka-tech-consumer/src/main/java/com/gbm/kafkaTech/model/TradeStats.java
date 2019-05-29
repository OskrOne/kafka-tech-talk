package com.gbm.kafkaTech.model;

public class TradeStats {
	private String type;
	private String ticker;
	private int countTrades;
	private double sumPrice;
	private double minPrice;
	private double avgPrice;
	
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public String getTicker() {
		return ticker;
	}
	public void setTicker(String ticker) {
		this.ticker = ticker;
	}
	public int getCountTrades() {
		return countTrades;
	}
	public void setCountTrades(int countTrades) {
		this.countTrades = countTrades;
	}
	public double getSumPrice() {
		return sumPrice;
	}
	public void setSumPrice(double sumPrice) {
		this.sumPrice = sumPrice;
	}
	public double getMinPrice() {
		return minPrice;
	}
	public void setMinPrice(double minPrice) {
		this.minPrice = minPrice;
	}
	public double getAvgPrice() {
		return avgPrice;
	}
	public void setAvgPrice(double avgPrice) {
		this.avgPrice = avgPrice;
	}
}
