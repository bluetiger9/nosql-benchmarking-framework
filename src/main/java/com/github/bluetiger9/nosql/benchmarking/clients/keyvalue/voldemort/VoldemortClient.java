package com.github.bluetiger9.nosql.benchmarking.clients.keyvalue.voldemort;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import voldemort.client.ClientConfig;
import voldemort.client.SocketStoreClientFactory;
import voldemort.client.StoreClient;
import voldemort.client.StoreClientFactory;

import com.github.bluetiger9.nosql.benchmarking.clients.ClientException;
import com.github.bluetiger9.nosql.benchmarking.clients.keyvalue.AbstractKeyValueStoreClient;

import static com.github.bluetiger9.nosql.benchmarking.clients.CommonClientProperties.*;

public class VoldemortClient extends AbstractKeyValueStoreClient {
	private static final String DEFAULT_SERVER = "tcp://localhost:6666/";

	private StoreClient<String, Object> client;
	
	private final List<String> servers;
	private final String bucket;
	
	public VoldemortClient(Properties props) {
		super(props);
		this.servers = Arrays.asList(properties.getProperty(PROPERTY_SERVERS, DEFAULT_SERVER).split(","));
		this.bucket = properties.getProperty(PROPERTY_BUCKET);
	}

	public void connect() throws ClientException {
		final StoreClientFactory factory = new SocketStoreClientFactory(new ClientConfig().setBootstrapUrls(servers));
		client = factory.getStoreClient(bucket);
	}

	public void disconnect() throws ClientException {
	}
	
	public void insert(String key, Object value) throws ClientException {
		client.put(key, value);
	}

	public Object get(String key) throws ClientException {
		return client.get(key);
	}

	public void update(String key, Object value) throws ClientException {
		insert(key, value);
	}

	public void delete(String key) throws ClientException {
		client.delete(key);
	}

}
