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
package com.github.bluetiger9.nosql.benchmarking.clients.document.couchdb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.ektorp.CouchDbConnector;
import org.ektorp.CouchDbInstance;
import org.ektorp.DocumentNotFoundException;
import org.ektorp.http.HttpClient;
import org.ektorp.http.StdHttpClient;
import org.ektorp.impl.StdCouchDbInstance;

import com.github.bluetiger9.nosql.benchmarking.Util;
import com.github.bluetiger9.nosql.benchmarking.clients.ClientException;
import com.github.bluetiger9.nosql.benchmarking.clients.document.AbstractDocumentStoreClient;

public class CouchDBClient extends AbstractDocumentStoreClient {
    private static final String PROPERTY_URL = "url";
    private static final String PROPERTY_DB = "db";
    
    private static final String DEFAULT_DB = "test";
    
    private final String url;
    private final String dbName;
    
    private HttpClient client;
    private CouchDbConnector db;
    
    public CouchDBClient(Properties props) {
        super(props);
        this.url = Util.getMandatoryProperty(properties, PROPERTY_URL);
        this.dbName = properties.getProperty(PROPERTY_DB, DEFAULT_DB);
    }

    @Override
    public void connect() throws ClientException {
        try {
            client = new StdHttpClient.Builder().url(url).build();
            final CouchDbInstance dbInstance = new StdCouchDbInstance(client);
            db = dbInstance.createConnector(dbName, true);
        } catch (Exception e) {
            throw new ClientException("connection error", e);
        }
    }
    
    @Override
    public void disconnect() throws ClientException {
        client.shutdown();
    }
    
    @Override
    public void insert(String key, Map<String, String> attributes) throws ClientException {
        final ObjectNode document = new ObjectNode(JsonNodeFactory.instance);
        for (Map.Entry<String, String> entry : attributes.entrySet()) {
            document.put(entry.getKey(), entry.getValue());
        }
        db.create(key, document);
    }

    @Override
    public Map<String, String> get(String key) throws ClientException {
        try {
            final JsonNode jsonNode = db.get(JsonNode.class, key);
            if (jsonNode == null)
                return null;
            
            final Map<String, String> result = new HashMap<>();
            final Iterator<Entry<String, JsonNode>> fields = jsonNode.getFields();
            while (fields.hasNext()) {
                final Entry<String, JsonNode> entry = fields.next();
                result.put(entry.getKey(), entry.getValue().toString());
            }
            return result;
        } catch (DocumentNotFoundException e) {
            return null;
        }
    }

    @Override
    public void update(String key, Map<String, String> attributes) throws ClientException {
        final ObjectNode jsonNode = (ObjectNode) db.get(JsonNode.class, key);        
        for (Map.Entry<String, String> entry : attributes.entrySet()) {
            jsonNode.put(entry.getKey(), entry.getValue());
        }
        db.update(jsonNode);
        
    }

    @Override
    public void delete(String key) throws ClientException {
        db.delete(db.get(JsonNode.class, key));        
    }


}
