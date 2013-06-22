package com.github.bluetiger9.nosql.benchmarking.benchmarks.keyvalue;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.math3.util.Pair;

import com.github.bluetiger9.nosql.benchmarking.Util;
import com.github.bluetiger9.nosql.benchmarking.benchmarks.GenericPerformanceBenchmark;
import com.github.bluetiger9.nosql.benchmarking.clients.ClientException;
import com.github.bluetiger9.nosql.benchmarking.clients.keyvalue.KeyValueStoreClient;
import com.github.bluetiger9.nosql.benchmarking.util.KeyGenerator;

public class GenericKeyValueStoreBenchmark extends GenericPerformanceBenchmark<KeyValueStoreClient> {
    private static final String OPERATION_READ = "read";
    private static final String OPERATION_INSERT = "insert";
    private static final String OPERATION_UPDATE = "update";
    private static final String OPERATION_DELETE = "delete";

    private final static int KEY_SIZE = 20;
    private final static int VALUE_SIZE = 1000;

    private final int initialRecords;

    private final List<Pair<String, Double>> availableOperations;

    public GenericKeyValueStoreBenchmark(Properties props) {
        super(props);
        this.initialRecords = Integer.parseInt(Util.getMandatoryProperty(props, "initialRecords"));
        final Double pRead = Double.parseDouble(props.getProperty("pRead", "0.0"));
        final Double pInsert = Double.parseDouble(props.getProperty("pInsert", "0.0"));
        final Double pUpdate = Double.parseDouble(props.getProperty("pUpdate", "0.0"));
        final Double pDelete = Double.parseDouble(props.getProperty("pDelete", "0.0"));
        if (Math.abs(pRead + pInsert + pUpdate + pDelete - 1.0) > 0.001) {
            throw new RuntimeException("The sum of operation probabilities must be 1.00");
        }        
        
        this.availableOperations = Arrays.asList(new Pair<>(OPERATION_READ, pRead), new Pair<>(OPERATION_INSERT,
                pInsert), new Pair<>(OPERATION_UPDATE, pUpdate), new Pair<>(OPERATION_DELETE, pDelete));
    }

    @Override
    protected List<Pair<String, Double>> getAvailableOperations() {
        return this.availableOperations;
    }

    @Override
    protected BenchmarkTask createBenchmarkTask() {
        final KeyValueBenchmarkTask task = new KeyValueBenchmarkTask();
        super.addBenchmarkTask(task);
        return task;
    }

    private class KeyValueBenchmarkTask extends GenericPerformanceBenchmark<KeyValueStoreClient>.BenchmarkTask {
        private final KeyGenerator keyGenerator;
        private final String keySufix;

        public KeyValueBenchmarkTask() {
            this.keyGenerator = new KeyGenerator();
            this.keySufix = Integer.toString(taskNr);
        }

        @Override
        public void doOperation(String op) throws ClientException {
            switch (op) {
            case OPERATION_READ:
                client.get(keyGenerator.getRandomKey());
                break;

            case OPERATION_INSERT:
                final String key = KeyGenerator.newRandomKey(KEY_SIZE) + keySufix;
                final String value = RandomStringUtils.randomAscii(VALUE_SIZE);
                client.insert(key, value);
                keyGenerator.add(key);
                break;

            case OPERATION_UPDATE:
                client.update(keyGenerator.getRandomKey(), RandomStringUtils.randomAscii(VALUE_SIZE));
                break;

            case OPERATION_DELETE:
                client.delete(keyGenerator.getRandomKey());
                break;
            }
        }

        public void init(KeyValueStoreClient client) {
            super.init(client);
            try {
                for (int i = 0; i < initialRecords / nrThreads; ++i) {
                    final String key = KeyGenerator.newRandomKey(KEY_SIZE) + keySufix;
                    final String value = RandomStringUtils.randomAscii(VALUE_SIZE);
                    client.insert(key, value);
                    keyGenerator.add(key);
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
