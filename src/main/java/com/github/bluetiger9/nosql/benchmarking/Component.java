package com.github.bluetiger9.nosql.benchmarking;

import java.util.Properties;

import org.apache.log4j.Logger;

public abstract class Component {
	private static final String PROPERTY_NAME = "name";
	private static final String PROPERTY_DESCRIPTION = "description";
	
	private static final String DEFAULT_DESCRIPTION = "";
	
	protected final Logger logger;
	protected final Properties properties;
	protected final String name;
	protected final String description;
	
	public Component() {
		this(new Properties());
	}
	
	public Component(Properties properties) {
		this.properties = properties;
		this.name = properties.getProperty(PROPERTY_NAME, getClass().getSimpleName());
		this.description = properties.getProperty(PROPERTY_DESCRIPTION, DEFAULT_DESCRIPTION);
		this.logger = Logger.getLogger(name);
		if (!properties.isEmpty()) {
		    logger.info("created with properties: " + properties);
		}
	}
	
	public String getName() {
		return name;
	}
	
	public String getDescription() {
		return description;
	}
}
