package com.github.bluetiger9.nosql.benchmarking.clients.column;

import com.github.bluetiger9.nosql.benchmarking.clients.AbstractDatabaseClient;

public abstract class AbstractColumnStoreClient extends AbstractDatabaseClient
		implements ColumnStoreClient {

	public AbstractColumnStoreClient(String name, String description) {
		super(name, description);
	}
	
}
