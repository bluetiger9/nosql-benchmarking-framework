package com.github.bluetiger9.nosql.benchmarking.clients.keyvalue.couchbase;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.github.bluetiger9.nosql.benchmarking.clients.keyvalue.memcached.MemcachedClient;

import com.github.bluetiger9.nosql.benchmarking.clients.ClientException;
import com.github.bluetiger9.nosql.benchmarking.clients.keyvalue.KeyValueStoreClient;

import static com.github.bluetiger9.nosql.benchmarking.clients.CommonClientProperties.*;

public class CouchbaseClient extends MemcachedClient implements
		KeyValueStoreClient {
	private static final String DEFAULT_BUCKET = "default";
	private static final String DEFAULT_PASSWORD = "";
	
	private final List<URI> servers = new ArrayList<URI>();
	private final String bucket;
	private final String password;
	
	public CouchbaseClient(final Properties props) {
		super(props);
		bucket = properties.getProperty(PROPERTY_BUCKET, DEFAULT_BUCKET);
		password = properties.getProperty(PROPERTY_PASSWORD, DEFAULT_PASSWORD);
		
		for (String server : properties.getProperty(PROPERTY_SERVERS).split(","))
			servers.add(URI.create(server));
	}

	public void connect() throws ClientException {
		try {
			client = new com.couchbase.client.CouchbaseClient(servers, bucket, password);
		} catch (IOException e) {
			throw new ClientException("connection error", e);
		}
	}
}
