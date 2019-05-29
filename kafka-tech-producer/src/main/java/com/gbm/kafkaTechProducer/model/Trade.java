package com.gbm.kafkaTechProducer.model;

public class Trade {
	
	private String type;
	private String ticker;
	private double price;
	private int size;
	
	public Trade(String type, String ticker, double price, int size) {
		this.type = type;
		this.ticker = ticker;
		this.price = price;
		this.size = size;
	}
	
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

	public double getPrice() {
		return price;
	}

	public void setPrice(double price) {
		this.price = price;
	}

	public int getSize() {
		return size;
	}

	public void setSize(int size) {
		this.size = size;
	}
	
	@Override
	public String toString() {
		return "Trade [type=" + type + ", ticker=" + ticker + ", price=" + price + ", size=" + size + "]";
	}	
}
