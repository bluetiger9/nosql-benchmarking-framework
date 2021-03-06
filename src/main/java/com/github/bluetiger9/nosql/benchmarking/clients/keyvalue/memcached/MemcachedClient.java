/*
 * Copyright (c) 2013 Attila Tőkés. All rights reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
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
