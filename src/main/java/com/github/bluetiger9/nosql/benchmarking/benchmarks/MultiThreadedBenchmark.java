package com.github.bluetiger9.nosql.benchmarking.benchmarks;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;

import com.github.bluetiger9.nosql.benchmarking.clients.ClientFactory;
import com.github.bluetiger9.nosql.benchmarking.clients.DatabaseClient;

public abstract class MultiThreadedBenchmark<ClientType extends DatabaseClient> extends AbstractBenchmark<ClientType> {

    protected final int nrThreads;
    private List<BenchmarkThread> benchmarkThreads;
    private CountDownLatch terminateCDL, initCDL, runCDL;
    
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
        
        logger.info("Exporting results...");
        try {            
            final File outputFolder = new File(properties.getProperty("outputFolder", "."));
            if (!outputFolder.exists()) {
                outputFolder.mkdir();
            }
            exportResults(outputFolder);
        } catch (IOException e) {
            logger.error("Result exporting failed: ", e);
        }
        
        logger.info("Exiting");
        System.exit(0);
    }
    
    private void createThreads(int nrThreads) {
        logger.info("Creating " + nrThreads + " benchmark threads.");
        terminateCDL = new CountDownLatch(nrThreads);
        initCDL = new CountDownLatch(nrThreads);
        runCDL = new CountDownLatch(nrThreads);
        benchmarkThreads = new ArrayList<>();
        for (int i = 0; i < nrThreads; i++) {
            benchmarkThreads.add(new BenchmarkThread(i, createBenchmarkTask(), clientFactory
                    .createClient()));
        }
    }
    
    protected abstract BenchmarkTask<ClientType> createBenchmarkTask();
    
    protected abstract void exportResults(File outputFolder) throws IOException;
    
    private void startThreads() {
        logger.info("Starting the benchmark threads...");
        for (Thread thread : benchmarkThreads) {
            thread.start();
        }
    }
    
    private void waitForTermination() {
        try {
            initCDL.await();
            logger.info("Initialization terminated on all threads.");
            
            logger.info("Running the benchmark tasks.");
            runCDL.await();
            logger.info("Benchmark tasks terminated on all threads.");            
            logger.info("Running the cleanup...");
            
            logger.info("Waiting for the benchmark threads to terminate.");
            terminateCDL.await();
            logger.info("All benchmark threads terminated");
        } catch (InterruptedException e) {
            logger.error("Benchmark execution interrupted", e);
        }
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
        
        private void doInit() {
            logger.info("Running initialization.");
            task.init(client);
            initCDL.countDown();
            
            try {
                logger.info("Waiting other threads to terminate the initialization.");
                initCDL.await();
            } catch (InterruptedException e1) {
                e1.printStackTrace();
            }
        }
        
        @Override
        public void run() {
            doInit();
            
            logger.info("Benchmark thread started");
            try {
                logger.info("Executing the benchmark task");
                task.run();
            } catch (Exception e) {
                logger.error("Error occured running the benchmark task.", e);
            }
            runCDL.countDown();
            logger.info("Benchmark task terminated");
            
            doCleanUp();
            
            // notify the master thread about the termination
            terminateCDL.countDown();
            logger.info("Exiting.");
        }
        
        private void doCleanUp() {
            try {
                logger.info("Waiting other threads to terminate the benchmark task");
                runCDL.await();
            } catch (InterruptedException e) {
                e.printStackTrace();                
            }
            
            logger.info("Running the cleanup.");
            task.cleanUp();       
        }
    }
   
    public interface BenchmarkTask<ClientType extends DatabaseClient> {
        void init(ClientType client);
        void run();
        void cleanUp();
    }
    
    public interface BenchmarkTaskFactory<ClientType extends DatabaseClient> {
        BenchmarkTask<ClientType> createBenchmarkTask();
    }

}
