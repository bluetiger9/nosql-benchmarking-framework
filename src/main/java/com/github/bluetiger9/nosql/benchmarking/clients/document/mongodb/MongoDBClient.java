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
package com.github.bluetiger9.nosql.benchmarking.clients.document.mongodb;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;

import com.github.bluetiger9.nosql.benchmarking.clients.ClientException;
import com.github.bluetiger9.nosql.benchmarking.clients.CommonClientProperties;
import com.github.bluetiger9.nosql.benchmarking.clients.document.AbstractDocumentStoreClient;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;

public class MongoDBClient extends AbstractDocumentStoreClient {
    private static final String PROPERTY_DB = "db";
    private static final String PROPERTY_COLLECTION = "collection";
    
    private static final String DEFAULT_SERVERS = "localhost:27018";
    private static final String DEFAULT_DB = "test";
    private static final String DEFAULT_COLLECTION = "test";

    private final List<ServerAddress> servers;
    private final String dbName;
    private final String collection;
    
    private MongoClient client;
    private DB db;
    private DBCollection dbCollection;

    public MongoDBClient(Properties props) {
        super(props);
        this.servers = new ArrayList<>();
        this.dbName = properties.getProperty(PROPERTY_DB, DEFAULT_DB);
        this.collection = properties.getProperty(PROPERTY_COLLECTION, DEFAULT_COLLECTION);
    }

    @Override
    public void connect() throws ClientException {
        try {
            for (String server : StringUtils.split(
                    properties.getProperty(CommonClientProperties.PROPERTY_SERVERS, DEFAULT_SERVERS), ',')) {
                servers.add(getServerAddress(server));
            }
            
            client = new MongoClient(servers);
            db = client.getDB(dbName);
            dbCollection = db.getCollection(collection);
        } catch (Exception e) {
            throw new ClientException("connection error", e);
        }
    }

    @Override
    public void disconnect() throws ClientException {
        client.close();
    }

    @Override
    public void insert(String key, Map<String, String> attributes) throws ClientException {
        final BasicDBObject doc = new BasicDBObject();
        doc.append("_id", key);
        for (Map.Entry<String, String> entry : attributes.entrySet()) {
            doc.append(entry.getKey(), entry.getValue());
        }
        dbCollection.insert(doc);
    }

    @Override
    public Map<String, String> get(String key) throws ClientException {
        final DBObject document = dbCollection.findOne(new BasicDBObject("_id", key));
        
        if (document == null)
            return null;                
        
        final Map<String, String> result = new HashMap<>();
        for (String attribute : document.keySet()) {
            result.put(attribute, document.get(attribute).toString());
        }
        return result;
    }

    @Override
    public void update(String key, Map<String, String> attributes) throws ClientException {
        final BasicDBObject doc = new BasicDBObject();
        for (Map.Entry<String, String> entry : attributes.entrySet()) {
            doc.append(entry.getKey(), entry.getValue());
        }
        dbCollection.update(new BasicDBObject("_id", key), new BasicDBObject("$set", doc));
    }

    @Override
    public void delete(String key) throws ClientException {
        dbCollection.remove(new BasicDBObject("_id", key));
    }

    private static ServerAddress getServerAddress(String server) throws UnknownHostException {
        final String host = StringUtils.substringBefore(server, ":");
        final String port = StringUtils.substringAfter(server, ":");
        if (!StringUtils.isBlank(port)) {
            return new ServerAddress(host, Integer.parseInt(port));
        } else {
            return new ServerAddress(host);
        }
    }

}
