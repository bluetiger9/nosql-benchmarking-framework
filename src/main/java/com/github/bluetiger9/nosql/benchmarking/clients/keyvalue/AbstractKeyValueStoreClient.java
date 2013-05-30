package com.github.bluetiger9.nosql.benchmarking.clients.keyvalue;

import java.util.Properties;

import com.github.bluetiger9.nosql.benchmarking.clients.AbstractDatabaseClient;

public abstract class AbstractKeyValueStoreClient extends
		AbstractDatabaseClient implements KeyValueStoreClient {

	public AbstractKeyValueStoreClient(final Properties properties) {
		super(properties);
	}

}
