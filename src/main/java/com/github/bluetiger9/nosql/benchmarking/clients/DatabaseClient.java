package com.github.bluetiger9.nosql.benchmarking.clients;

public interface DatabaseClient {
	
	String getName();
	
	String getDescription();
	
	void connect();
	
	void disconnect();
}
