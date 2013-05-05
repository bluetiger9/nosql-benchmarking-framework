package com.github.bluetiger9.nosql.benchmarking.clients.keyvalue;

import com.github.bluetiger9.nosql.benchmarking.clients.DatabaseClient;

public interface KeyValueStoreClient extends DatabaseClient {
	
	void insert(String key, Object value);
	
	Object get(String key);
	
	void update(String key, Object value);
	
	void delete(String key);
}
