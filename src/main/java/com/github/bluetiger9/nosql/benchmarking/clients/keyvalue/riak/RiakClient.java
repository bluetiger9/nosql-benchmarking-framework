package com.github.bluetiger9.nosql.benchmarking.clients.keyvalue.riak;

import static com.github.bluetiger9.nosql.benchmarking.clients.CommonClientProperties.PROPERTY_BUCKET;
import static com.github.bluetiger9.nosql.benchmarking.clients.CommonClientProperties.PROPERTY_HOST;
import static com.github.bluetiger9.nosql.benchmarking.clients.CommonClientProperties.PROPERTY_PORT;

import java.util.Properties;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.IRiakObject;
import com.basho.riak.client.RiakFactory;
import com.basho.riak.client.bucket.Bucket;
import com.github.bluetiger9.nosql.benchmarking.clients.ClientException;
import com.github.bluetiger9.nosql.benchmarking.clients.keyvalue.AbstractKeyValueStoreClient;

public class RiakClient extends AbstractKeyValueStoreClient {
	private static final String DEFAULT_HOST = "localhost";
	private static final String DEFUALT_PORT = "8087";
	
	private final String host;
	private final int port;
	private final String bucketName;
	
	private IRiakClient client;
	private Bucket bucket;
	
	public RiakClient(Properties properties) {
		super(properties);
		this.host = properties.getProperty(PROPERTY_HOST, DEFAULT_HOST);
		this.port = Integer.parseInt(properties.getProperty(PROPERTY_PORT, DEFUALT_PORT));
		this.bucketName = properties.getProperty(PROPERTY_BUCKET);
	}
	
	public void connect() throws ClientException {
		try {
			client = RiakFactory.pbcClient(host, port);
			bucket = client.fetchBucket(bucketName).execute();
		} catch (Exception e) {
			throw new ClientException("connection error", e);
		}
	}

	public void disconnect() throws ClientException {
		client.shutdown();
	}

	public void insert(String key, Object value) throws ClientException {
		try {
			bucket.store(key, value.toString()).execute();
		} catch (Exception e) {
			throw new ClientException(e);
		}
	}

	public IRiakObject get(String key) throws ClientException {
		try {
			return bucket.fetch(key).execute();
		} catch (Exception e) {
			throw new ClientException(e);
		}
	}

	public void update(String key, Object value) throws ClientException {
		insert(key, value);
	}

	public void delete(String key) throws ClientException {
		bucket.delete(key);
	}

}
