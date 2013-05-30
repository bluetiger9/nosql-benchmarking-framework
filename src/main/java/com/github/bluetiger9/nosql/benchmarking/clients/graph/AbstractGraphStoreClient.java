package com.github.bluetiger9.nosql.benchmarking.clients.graph;

import java.util.Properties;

import com.github.bluetiger9.nosql.benchmarking.clients.AbstractDatabaseClient;

public abstract class AbstractGraphStoreClient extends AbstractDatabaseClient
		implements GraphStoreClient {

	public AbstractGraphStoreClient(final Properties properties) {
		super(properties);
	}
	
}
