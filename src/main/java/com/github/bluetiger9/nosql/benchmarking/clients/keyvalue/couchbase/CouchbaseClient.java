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
