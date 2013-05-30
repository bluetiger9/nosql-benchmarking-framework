package com.github.bluetiger9.nosql.benchmarking.clients.column;

import java.util.Properties;

import com.github.bluetiger9.nosql.benchmarking.clients.AbstractDatabaseClient;

public abstract class AbstractColumnStoreClient extends AbstractDatabaseClient
		implements ColumnStoreClient {

	public AbstractColumnStoreClient(final Properties properties) {
		super(properties);
	}	
}
