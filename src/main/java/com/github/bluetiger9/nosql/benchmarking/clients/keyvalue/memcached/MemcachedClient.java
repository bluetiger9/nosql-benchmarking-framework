package com.github.bluetiger9.nosql.benchmarking.clients.keyvalue.memcached;

import java.net.InetSocketAddress;
import java.util.Properties;

import com.github.bluetiger9.nosql.benchmarking.clients.ClientException;
import com.github.bluetiger9.nosql.benchmarking.clients.keyvalue.AbstractKeyValueStoreClient;

public class MemcachedClient extends AbstractKeyValueStoreClient {
	private static final String PROPERTY_HOST = "host";
	private static final String PROPERTY_PORT = "port";
	private static final String PROPERTY_EXPIRE_TIME = "expireTime";
	
	private static final String DEFAULT_HOST = "localhost";
	private static final String DEFAULT_PORT = "11211";
	private static final String DEFAULT_EXPIRE_TIME = "0"; // store forever

	protected net.spy.memcached.MemcachedClient client;
	private final String host;
	private final int port;
	private final int expireTime;
	
	public MemcachedClient(Properties properties) {
		super(properties);
		this.host = properties.getProperty(PROPERTY_HOST, DEFAULT_HOST);
		this.port = Integer.parseInt(properties.getProperty(PROPERTY_PORT, DEFAULT_PORT));
		this.expireTime = Integer.parseInt(properties.getProperty(PROPERTY_EXPIRE_TIME, DEFAULT_EXPIRE_TIME)); 
	}
	
	public void connect() throws ClientException {
		try {
			client = new net.spy.memcached.MemcachedClient(new InetSocketAddress(host, port));
		} catch (Exception e) {
			throw new ClientException("connection error", e);
		}
	}

	public void disconnect() throws ClientException {
		client.shutdown();
	}

	public void insert(String key, Object value) throws ClientException {
		try {
			client.set(key, expireTime, value).get();
		} catch (Exception e) {
			throw new ClientException(e);
		}
	}

	public Object get(String key) throws ClientException {
		return client.get(key);
	}

	public void update(String key, Object value) throws ClientException {
		insert(key, value);
	}

	public void delete(String key) throws ClientException {
		try {
			client.delete(key).get();
		} catch (Exception e) {
			throw new ClientException(e);
		}
	}

}
