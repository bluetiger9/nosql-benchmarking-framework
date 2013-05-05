package com.github.bluetiger9.nosql.benchmarking.clients.keyvalue;

import com.github.bluetiger9.nosql.benchmarking.clients.AbstractDatabaseClient;

public abstract class AbstractKeyValueStoreClient extends
		AbstractDatabaseClient implements KeyValueStoreClient {

	public AbstractKeyValueStoreClient(String name, String description) {
		super(name, description);
	}

}
