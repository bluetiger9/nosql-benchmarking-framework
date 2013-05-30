package com.github.bluetiger9.nosql.benchmarking.clients.document;

import java.util.Properties;

import com.github.bluetiger9.nosql.benchmarking.clients.AbstractDatabaseClient;

public abstract class AbstractDocumentStoreClient extends AbstractDatabaseClient
		implements DocumentStoreClient {

	public AbstractDocumentStoreClient(final Properties properties) {
		super(properties);
	}
	
}
