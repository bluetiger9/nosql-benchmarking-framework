package com.github.bluetiger9.nosql.benchmarking.clients.graph;

import com.github.bluetiger9.nosql.benchmarking.clients.AbstractDatabaseClient;

public abstract class AbstractGraphStoreClient extends AbstractDatabaseClient
		implements GraphStoreClient {

	public AbstractGraphStoreClient(String name, String description) {
		super(name, description);
	}
	
}
