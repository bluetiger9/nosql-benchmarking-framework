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
package com.github.bluetiger9.nosql.benchmarking.benchmarks.document;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.math3.util.Pair;

import com.github.bluetiger9.nosql.benchmarking.Util;
import com.github.bluetiger9.nosql.benchmarking.benchmarks.GenericPerformanceBenchmark;
import com.github.bluetiger9.nosql.benchmarking.clients.ClientException;
import com.github.bluetiger9.nosql.benchmarking.clients.document.DocumentStoreClient;
import com.github.bluetiger9.nosql.benchmarking.util.KeyGenerator;

public class GenericDocumentStoreBenchmark extends GenericPerformanceBenchmark<DocumentStoreClient> {
    private static final String OPERATION_READ = "read";
    private static final String OPERATION_INSERT = "insert";
    private static final String OPERATION_UPDATE = "update";
    private static final String OPERATION_DELETE = "delete";

    private final static int KEY_SIZE = 20;
    private final static int VALUE_SIZE = 100;
    private final static int ATTRIBUTE_NAME_SIZE = 10;

    private final int initialRecords;
    private final int nrAttributes;
    private final int nrAttributesUpdate;
    private final List<Pair<String, Double>> availableOperations;
    private final List<String> attributes;

    public GenericDocumentStoreBenchmark(Properties props) {
        super(props);
        this.initialRecords = Integer.parseInt(Util.getMandatoryProperty(props, "initialRecords"));
        this.nrAttributes = Integer.parseInt(Util.getMandatoryProperty(properties, "nrAttributes"));
        this.nrAttributesUpdate = Integer.parseInt(Util.getMandatoryProperty(properties, "nrAttributesUpdate"));
        
        final Double pRead = Double.parseDouble(props.getProperty("pRead", "0.0"));
        final Double pInsert = Double.parseDouble(props.getProperty("pInsert", "0.0"));
        final Double pUpdate = Double.parseDouble(props.getProperty("pUpdate", "0.0"));
        final Double pDelete = Double.parseDouble(props.getProperty("pDelete", "0.0"));
        if (Math.abs(pRead + pInsert + pUpdate + pDelete - 1.0) > 0.001) {
            throw new RuntimeException("The sum of operation probabilities must be 1.00");
        }        
        
        this.availableOperations = Arrays.asList(new Pair<>(OPERATION_READ, pRead), new Pair<>(OPERATION_INSERT,
                pInsert), new Pair<>(OPERATION_UPDATE, pUpdate), new Pair<>(OPERATION_DELETE, pDelete));
        
        this.attributes = new ArrayList<String>();
        for (int i = 0; i < nrAttributes; ++i) {
            attributes.add(RandomStringUtils.randomAlphanumeric(ATTRIBUTE_NAME_SIZE));
        }
    }

    @Override
    protected List<Pair<String, Double>> getAvailableOperations() {
        return this.availableOperations;
    }

    @Override
    protected BenchmarkTask createBenchmarkTask() {
        final DocumentStoreBenchmarkTask task = new DocumentStoreBenchmarkTask();
        super.addBenchmarkTask(task);
        return task;
    }

    private class DocumentStoreBenchmarkTask extends GenericPerformanceBenchmark<DocumentStoreClient>.BenchmarkTask {
        private final KeyGenerator keyGenerator;
        private final String keySufix;
        private final List<String> attributeList;

        public DocumentStoreBenchmarkTask() {
            this.keyGenerator = new KeyGenerator();
            this.keySufix = Integer.toString(taskNr);
            this.attributeList = new ArrayList<>(attributes);
        }

        @Override
        public void doOperation(String op) throws ClientException {
            switch (op) {
            case OPERATION_READ:
                client.get(keyGenerator.getRandomKey());
                break;

            case OPERATION_INSERT:
                final String key = KeyGenerator.newRandomKey(KEY_SIZE) + keySufix;
                final Map<String, String> values = new HashMap<>();
                for (String attribute : attributes) {
                    values.put(attribute, RandomStringUtils.randomAscii(VALUE_SIZE));
                }
                client.insert(key, values);
                keyGenerator.add(key);
                break;

            case OPERATION_UPDATE:
                Collections.shuffle(attributeList);
                final Map<String, String> updateValues = new HashMap<>();
                for (int i = 0; i < nrAttributesUpdate; ++i) {
                    updateValues.put(attributes.get(i), RandomStringUtils.randomAscii(VALUE_SIZE));
                }
                client.update(keyGenerator.getRandomKey(), updateValues);
                break;

            case OPERATION_DELETE:
                client.delete(keyGenerator.getRandomKey());
                break;
            }
        }

        public void init(DocumentStoreClient client) {
            super.init(client);
            try {
                for (int i = 0; i < initialRecords / nrThreads; ++i) {
                    doOperation(OPERATION_INSERT);
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
                    client.delete(key);
                }
            } catch (ClientException e) {
                logger.error("Error occurred when cleaning the test data");
            }
            
            super.cleanUp();
        }
    }

}
