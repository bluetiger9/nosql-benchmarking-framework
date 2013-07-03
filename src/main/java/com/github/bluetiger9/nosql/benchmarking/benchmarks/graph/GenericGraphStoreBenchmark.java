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
package com.github.bluetiger9.nosql.benchmarking.benchmarks.graph;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.math3.util.Pair;

import com.github.bluetiger9.nosql.benchmarking.Util;
import com.github.bluetiger9.nosql.benchmarking.benchmarks.GenericPerformanceBenchmark;
import com.github.bluetiger9.nosql.benchmarking.clients.ClientException;
import com.github.bluetiger9.nosql.benchmarking.clients.graph.GraphStoreClient;
import com.github.bluetiger9.nosql.benchmarking.util.KeyGenerator;

public class GenericGraphStoreBenchmark extends GenericPerformanceBenchmark<GraphStoreClient> {
    private static final String OPERATION_INSERT_NODE = "insert_node";
    private static final String OPERATION_INSERT_EDGE = "insert_edge";
    private static final String OPERATION_GET_NODE = "get_node";
    private static final String OPERATION_GET_EDGE = "get_edge";
    private static final String OPERATION_DELETE_NODE = "delete_node";
    private static final String OPERATION_DELETE_EDGE = "delete_edge";    

    private final static int KEY_SIZE = 20;
    private final static int VALUE_SIZE = 50;
    private final static int NR_PROPERTIES = 5;
    private final static int PROPERTY_NAME_LENGTH = 10;

    private final int initialRecords;
    private final List<Pair<String, Double>> availableOperations;

    public GenericGraphStoreBenchmark(Properties props) {
        super(props);
        this.initialRecords = Integer.parseInt(Util.getMandatoryProperty(props, "initialRecords"));
        
        final Double pInsertNode = Double.parseDouble(props.getProperty("pInsertNode", "0.0"));
        final Double pInsertEdge = Double.parseDouble(props.getProperty("pInsertEdge", "0.0"));
        final Double pGetNode = Double.parseDouble(props.getProperty("pGetNode", "0.0"));
        final Double pGetEdge = Double.parseDouble(props.getProperty("pGetEdge", "0.0"));
        final Double pDeleteNode = Double.parseDouble(props.getProperty("pDeleteNode", "0.0"));
        final Double pDeleteEdge = Double.parseDouble(props.getProperty("pDeleteEdge", "0.0"));
        if (Math.abs(pInsertNode + pInsertEdge + pGetNode + pGetEdge + pDeleteNode + pDeleteEdge - 1.0) > 0.001) {
            throw new RuntimeException("The sum of operation probabilities must be 1.00");
        }        
        
        this.availableOperations = Arrays.asList(
                new Pair<>(OPERATION_INSERT_NODE, pInsertNode), 
                new Pair<>(OPERATION_INSERT_EDGE, pInsertEdge),
                new Pair<>(OPERATION_GET_NODE, pGetNode), 
                new Pair<>(OPERATION_GET_EDGE, pGetEdge),
                new Pair<>(OPERATION_DELETE_NODE, pDeleteNode),
                new Pair<>(OPERATION_DELETE_EDGE, pDeleteEdge));
    }

    @Override
    protected List<Pair<String, Double>> getAvailableOperations() {
        return this.availableOperations;
    }

    @Override
    protected BenchmarkTask createBenchmarkTask() {
        final GraphStoreBenchmarkTask task = new GraphStoreBenchmarkTask();
        super.addBenchmarkTask(task);
        return task;
    }

    private class GraphStoreBenchmarkTask extends GenericPerformanceBenchmark<GraphStoreClient>.BenchmarkTask {
        private final KeyGenerator keyGenerator;
        private final String keySufix;

        public GraphStoreBenchmarkTask() {
            this.keyGenerator = new KeyGenerator();
            this.keySufix = Integer.toString(taskNr);
        }

        @Override
        public void doOperation(String op) throws ClientException {
            switch (op) {
            case OPERATION_GET_NODE:
                client.getNode(keyGenerator.getRandomKey());
                break;
                
            case OPERATION_GET_EDGE:
                client.getEdge(keyGenerator.getRandomKey(), keyGenerator.getRandomKey());
                break;
                
            case OPERATION_INSERT_NODE:
                final String key = KeyGenerator.newRandomKey(KEY_SIZE) + keySufix;
                final Map<String, String> values = new HashMap<>();
                for (int i = 0; i < NR_PROPERTIES; ++i) {
                    values.put(RandomStringUtils.randomAlphanumeric(PROPERTY_NAME_LENGTH), RandomStringUtils.randomAlphanumeric(VALUE_SIZE));
                }
                client.insertNode(key, values);
                keyGenerator.add(key);
                break;

            case OPERATION_INSERT_EDGE:
                final String key1 = keyGenerator.getRandomKey();
                final String key2 = keyGenerator.getRandomKey();
                final Map<String, String> edgeValues = new HashMap<>();
                for (int i = 0; i < NR_PROPERTIES; ++i) {
                    edgeValues.put(RandomStringUtils.randomAlphanumeric(PROPERTY_NAME_LENGTH), RandomStringUtils.randomAlphanumeric(VALUE_SIZE));
                }
                client.insertEdge(key1, key2, edgeValues);
                break;

            case OPERATION_DELETE_NODE:
                client.deleteNode(keyGenerator.getRandomKey());
                break;
                
            case OPERATION_DELETE_EDGE:
                client.deleteEdge(keyGenerator.getRandomKey(), keyGenerator.getRandomKey());
                break;
            }
        }

        public void init(GraphStoreClient client) {
            super.init(client);
            try {
                for (int i = 0; i < initialRecords / nrThreads; ++i) {
                    doOperation(OPERATION_INSERT_NODE);
                }
                
                for (int i = 0; i < initialRecords / nrThreads / 10; ++i) {
                    doOperation(OPERATION_INSERT_EDGE);
                }
            } catch (ClientException e) {
                logger.error("Error occurred when inserting test data in the database.", e);
            }
        }

        @Override
        public void cleanUp() {
            logger.info("Cleaning up test data.");

            try {
                for (String key : keyGenerator.getAllKeys()) {
                    client.deleteNode(key);
                }
            } catch (ClientException e) {
                logger.error("Error occurred when cleaning the test data");
            }
            
            super.cleanUp();
        }
    }

}
