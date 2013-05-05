package com.github.bluetiger9.nosql.benchmarking.clients.document;

import com.github.bluetiger9.nosql.benchmarking.clients.AbstractDatabaseClient;

public abstract class AbstractDocumentStoreClient extends AbstractDatabaseClient
		implements DocumentStoreClient {

	public AbstractDocumentStoreClient(String name, String description) {
		super(name, description);
	}
	
}
