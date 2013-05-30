package com.github.bluetiger9.nosql.benchmarking.clients.column;

import com.github.bluetiger9.nosql.benchmarking.clients.ClientException;
import com.github.bluetiger9.nosql.benchmarking.clients.DatabaseClient;

public interface ColumnStoreClient extends DatabaseClient {
	
	void insert(String key, String columnFamily, String column, String value) throws ClientException;
	
	String get(String key, String columnFamily, String column) throws ClientException;

}
