package com.github.bluetiger9.nosql.benchmarking.clients;

import java.util.Properties;

import com.github.bluetiger9.nosql.benchmarking.Component;

public abstract class AbstractDatabaseClient extends Component implements DatabaseClient {
	public AbstractDatabaseClient(Properties properties) {
		super(properties);
	}
}
