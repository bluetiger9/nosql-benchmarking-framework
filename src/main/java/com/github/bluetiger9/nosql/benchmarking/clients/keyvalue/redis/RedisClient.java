package com.github.bluetiger9.nosql.benchmarking.clients.keyvalue.redis;

import java.util.Properties;

import redis.clients.jedis.Jedis;

import com.github.bluetiger9.nosql.benchmarking.clients.ClientException;
import com.github.bluetiger9.nosql.benchmarking.clients.keyvalue.AbstractKeyValueStoreClient;

import static com.github.bluetiger9.nosql.benchmarking.clients.CommonClientProperties.*;

public class RedisClient extends AbstractKeyValueStoreClient {
	private static final String DEFAULT_HOST = "localhost";
	private static final String DEFAULT_PORT = "6379";
	
	private Jedis client;
	
	private final String host;
	private final int port;

	public RedisClient(Properties props) {
		super(props);
		this.host = properties.getProperty(PROPERTY_HOST, DEFAULT_HOST);
		this.port = Integer.parseInt(properties.getProperty(PROPERTY_PORT, DEFAULT_PORT));
	}

	public void connect() throws ClientException {
		client = new Jedis(host, port);
	}

	public void disconnect() throws ClientException {
		client.disconnect();
	}
	
	public void insert(String key, Object value) throws ClientException {
		client.set(key, value.toString());
	}

	public Object get(String key) throws ClientException {
		return client.get(key);
	}

	public void update(String key, Object value) throws ClientException {
		insert(key, value);
	}

	public void delete(String key) throws ClientException {
		client.del(key);
	}

}
