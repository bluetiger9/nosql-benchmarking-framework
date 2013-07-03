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
package com.github.bluetiger9.nosql.benchmarking.clients.graph.orientdb;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.github.bluetiger9.nosql.benchmarking.Util;
import com.github.bluetiger9.nosql.benchmarking.clients.ClientException;
import com.github.bluetiger9.nosql.benchmarking.clients.graph.AbstractGraphStoreClient;
import com.orientechnologies.orient.core.db.graph.OGraphDatabase;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.dictionary.ODictionary;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;

public class OrientDBClient extends AbstractGraphStoreClient {
    private static final String PROPERTY_URL = "url";
    private static final String PROPERTY_USER = "user";
    private static final String PROPERTY_PASSWORD = "password";

    private static final String DEFAULT_USER = "admin";
    private static final String DEFAULT_PASSWORD = "admin";

    private final String url;
    private final String username;
    private final String password;

    private OGraphDatabase db;
    private ODictionary<ORecordInternal<?>> dictionary;

    public OrientDBClient(Properties properties) {
        super(properties);
        this.url = Util.getMandatoryProperty(properties, PROPERTY_URL);
        this.username = properties.getProperty(PROPERTY_USER, DEFAULT_USER);
        this.password = properties.getProperty(PROPERTY_PASSWORD, DEFAULT_PASSWORD);
    }

    @Override
    public void connect() throws ClientException {
        db = new OGraphDatabase(url).open(username, password);
        dictionary = db.getDictionary();
        if ("true".equals(properties.getProperty("declareInsertIntent"))) {
            logger.info("Using insert intent");
            db.declareIntent(new OIntentMassiveInsert());
        }
    }

    @Override
    public void disconnect() throws ClientException {
        db.close();
    }

    @Override
    public void insertNode(String key, Map<String, String> attributes) throws ClientException {
        final ODocument vertex = db.createVertex();
        for (Map.Entry<String, String> attr : attributes.entrySet()) {
            vertex.field(attr.getKey(), attr.getValue());
        }
        dictionary.put(key, vertex);
    }

    @Override
    public void insertEdge(String key1, String key2, Map<String, String> attributes) throws ClientException {
        final ODocument edge = db.createEdge((ODocument) dictionary.get(key1), (ODocument) dictionary.get(key2));
        for (Map.Entry<String, String> attr : attributes.entrySet()) {
            edge.field(attr.getKey(), attr.getValue());
        }
    }

    @Override
    public Map<String, String> getNode(String key) {
        final ODocument document = dictionary.get(key);
        if (document == null)
            return null;
        
        final Map<String, String> map = new HashMap<>();
        for (String attr : document.fieldNames()) {
            map.put(attr, document.field(attr).toString());            
        }
        return map;
    }

    @Override
    public Map<String, String> getEdge(String key1, String key2) {
        final ODocument edge = getEdgeInternal(key1, key2);
        if (edge == null)
            return null;
        
        final Map<String, String> map = new HashMap<>();
        for (String attr : edge.fieldNames()) {
            map.put(attr, edge.field(attr).toString());            
        }
        return map;
    }

    private ODocument getEdgeInternal(String key1, String key2) {
        final Set<OIdentifiable> edgesBetweenVertexes = db.getEdgesBetweenVertexes(dictionary.get(key1), dictionary.get(key2));
        if (edgesBetweenVertexes.isEmpty())
            return null;
        
        final ODocument edge = (ODocument) edgesBetweenVertexes.iterator().next();
        return edge;
    }

    @Override
    public void deleteNode(String key) throws ClientException {
        dictionary.remove(key);
    }

    @Override
    public void deleteEdge(String key1, String key2) throws ClientException {
        db.removeVertex(getEdgeInternal(key1, key2));
    }

}
