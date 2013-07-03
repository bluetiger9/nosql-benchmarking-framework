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
package com.github.bluetiger9.nosql.benchmarking.clients.graph.neo4j;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.index.Index;
import org.neo4j.rest.graphdb.RestGraphDatabase;

import com.github.bluetiger9.nosql.benchmarking.Util;
import com.github.bluetiger9.nosql.benchmarking.clients.ClientException;
import com.github.bluetiger9.nosql.benchmarking.clients.graph.AbstractGraphStoreClient;

public class Neo4jClient extends AbstractGraphStoreClient {
    private static final String PROPERTY_URL = "url";
    
    private static final String INDEX_NAME = "keys";
    private static final String INDEX_KEY = "key";
    
    private static final RelationshipType RELATIONSHIP_TYPE = DynamicRelationshipType.withName("edge");
    
    private final String url;
    
    private GraphDatabaseService client;
    private Index<Node> index;
    
    public Neo4jClient(Properties props) {
        super(props);
        this.url = Util.getMandatoryProperty(props, PROPERTY_URL);
    }
    
    @Override
    public void connect() throws ClientException {
        client = new RestGraphDatabase(url);
        index = client.index().forNodes(INDEX_NAME);
    }

    @Override
    public void disconnect() throws ClientException {
        client.shutdown();
    }

    @Override
    public void insertNode(String key, Map<String, String> attributes) throws ClientException {
        final Node node = client.createNode();
        for (Map.Entry<String, String> entry : attributes.entrySet()) {
            node.setProperty(entry.getKey(), entry.getValue());
        }
        index.add(node, "key", key);
    }

    @Override
    public void insertEdge(String key1, String key2, Map<String, String> attributes) throws ClientException {
        final Node node1 = index.get(INDEX_KEY, key1).getSingle();
        final Node node2 = index.get(INDEX_KEY, key2).getSingle();
        final Relationship edge = node1.createRelationshipTo(node2, RELATIONSHIP_TYPE);
        for (Map.Entry<String, String> entry : attributes.entrySet()) {
            edge.setProperty(entry.getKey(), entry.getValue());
        }               
    }

    @Override
    public Map<String, String> getNode(String key) {
        final Node node = index.get(INDEX_KEY, key).getSingle();
        if (node == null)
            return null;
        final Map<String, String> result = new HashMap<>();
        for (String prop : node.getPropertyKeys()) {
            result.put(prop, node.getProperty(prop).toString());
        }
        return result;
    }

    @Override
    public Map<String, String> getEdge(String key1, String key2) {
        final Node node1 = index.get(INDEX_KEY, key1).getSingle();
        final Node node2 = index.get(INDEX_KEY, key2).getSingle();
        final Relationship rel = findEdge(node1, node2);
        if (rel != null) {
            final Map<String, String> result = new HashMap<>();
            for (String prop : rel.getPropertyKeys()) {
                result.put(prop, rel.getProperty(prop).toString());
            }
            return result;
        }
        return null;
    }
    
    Relationship findEdge(Node node1, Node node2) {
        for (Relationship rel : node1.getRelationships(RELATIONSHIP_TYPE)) {
            if (rel.getEndNode().equals(node2)) {
                return rel;
            }
        }
        return null;
    }

    @Override
    public void deleteNode(String key) throws ClientException {
        index.get(INDEX_KEY, key).getSingle().delete();
    }

    @Override
    public void deleteEdge(String key1, String key2) throws ClientException {
        final Node node1 = index.get(INDEX_KEY, key1).getSingle();
        final Node node2 = index.get(INDEX_KEY, key2).getSingle();
        final Relationship rel = findEdge(node1, node2);
        if (rel != null) {
            rel.delete();
        }
    }
}

