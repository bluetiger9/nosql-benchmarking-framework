package com.github.bluetiger9.nosql.benchmarking.benchmarks;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;

import com.github.bluetiger9.nosql.benchmarking.clients.ClientFactory;
import com.github.bluetiger9.nosql.benchmarking.clients.DatabaseClient;

public abstract class MultiThreadedBenchmark<ClientType extends DatabaseClient> extends AbstractBenchmark<ClientType> {

    private final int nrThreads;
    
    private BenchmarkTask<ClientType> benchmarkTask;    
    private List<BenchmarkThread> benchmarkThreads;
    private CountDownLatch countDownLatch;
    
    public MultiThreadedBenchmark(Properties props) {
        super(props);
        nrThreads = Integer.parseInt(properties.getProperty("nrThreads"));
        
    }

    @Override
    public void run(ClientFactory<? extends ClientType> clientFactory) {
        initBenchmark(clientFactory);

        createThreads(nrThreads);
        startThreads();
        waitForTermination();
    }
    
    private void createThreads(int nrThreads) {
        logger.info("Creating " + nrThreads + " benchmark threads.");
        countDownLatch = new CountDownLatch(nrThreads);
        benchmarkThreads = new ArrayList<>();
        for (int i = 0; i < nrThreads; i++) {
            benchmarkThreads.add(new BenchmarkThread(i, benchmarkTask, clientFactory.createClient()));
        }
    }
    
    private void startThreads() {
        logger.info("Starting the benchmark thread.");
        for (Thread thread : benchmarkThreads) {
            thread.start();
        }        
    }
    
    private void waitForTermination() {
        logger.info("Waiting for the benchmark threads to terminate.");
        try {
            countDownLatch.await();
            logger.info("All benchmark threads terminated");
        } catch (InterruptedException e) {
            logger.error("Benchmark execution interrupted", e);
        }
    }
    
    public void setBenchmarkTask(BenchmarkTask<ClientType> benchmarkTask) {
        this.benchmarkTask = benchmarkTask;
    }
    
    private class BenchmarkThread extends Thread {
        private final Logger logger;
        private final ClientType client;
        private final BenchmarkTask<ClientType> task;

        public BenchmarkThread(int id, BenchmarkTask<ClientType> benchmarkTask, ClientType client) {
            super("BenchmarkThread-" + id);
            this.logger = Logger.getLogger(getName());
            this.task = benchmarkTask;
            this.client = client;
        }
        
        @Override
        public void run() {
            logger.info("Benchmark thread started");            
            try {
                logger.info("Executing the benchmark task");
                task.run(client);
            } catch (Exception e) {
                logger.error("Error occured running the benchmark task.", e);
            }
            logger.info("Benchmark task terminated");
            
            // notify the master thread about the termination
            countDownLatch.countDown();
            logger.info("Exiting.");
        }
    }
    
    public interface BenchmarkTask<ClientType extends DatabaseClient> {
        void run(ClientType client);
    }          

}
