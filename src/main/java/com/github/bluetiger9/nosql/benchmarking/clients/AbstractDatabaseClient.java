package com.github.bluetiger9.nosql.benchmarking.clients;

public abstract class AbstractDatabaseClient implements DatabaseClient {
	private final String name;
	private final String description;

	public AbstractDatabaseClient(String name, String description) {
		this.name = name;
		this.description = description;
	}
	
	public String getName() {
		return name;
	}
	
	public String getDescription() {
		return description;
	};

}
