package com.github.bluetiger9.nosql.benchmarking.clients.keyvalue;

import com.github.bluetiger9.nosql.benchmarking.clients.ClientException;
import com.github.bluetiger9.nosql.benchmarking.clients.DatabaseClient;

public interface KeyValueStoreClient extends DatabaseClient {
	
	void insert(String key, Object value) throws ClientException;
	
	Object get(String key) throws ClientException;
	
	void update(String key, Object value) throws ClientException;
	
	void delete(String key) throws ClientException;
}
