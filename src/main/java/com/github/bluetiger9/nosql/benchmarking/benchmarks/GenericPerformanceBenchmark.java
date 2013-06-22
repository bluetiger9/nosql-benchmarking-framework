package com.github.bluetiger9.nosql.benchmarking.benchmarks;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.math3.distribution.EnumeratedDistribution;
import org.apache.commons.math3.util.Pair;
import org.apache.log4j.Logger;

import com.github.bluetiger9.nosql.benchmarking.Util;
import com.github.bluetiger9.nosql.benchmarking.clients.ClientException;
import com.github.bluetiger9.nosql.benchmarking.clients.DatabaseClient;
import com.github.bluetiger9.nosql.benchmarking.clients.keyvalue.KeyValueStoreClient;
import com.github.bluetiger9.nosql.benchmarking.util.ExportTools;
import com.github.bluetiger9.nosql.benchmarking.util.StopWatch;
import com.github.bluetiger9.nosql.benchmarking.util.TimeMeasurment;

public abstract class GenericPerformanceBenchmark<ClientType extends DatabaseClient> extends MultiThreadedBenchmark<ClientType> {  
    private final AtomicInteger taskCount = new AtomicInteger(0);
    private final int numOps;
    private final int throughput;
    
    private final List<BenchmarkTask> benchmarkTasks;

    public GenericPerformanceBenchmark(Properties props) {
        super(props);
        this.numOps = Integer.parseInt(Util.getMandatoryProperty(props, "operations"));
        this.throughput = Integer.parseInt(Util.getMandatoryProperty(props, "throughput"));
        this.benchmarkTasks = new ArrayList<>();
    }
    
    protected abstract List<Pair<String, Double>> getAvailableOperations();
    
    protected void addBenchmarkTask(BenchmarkTask task) {
        this.benchmarkTasks.add(task);
    }
    
    @Override
    protected void exportResults(File outputFolder) throws IOException {
        final PrintWriter writer = new PrintWriter(new File(outputFolder, "summary.txt"));
        writer.println("Operations: " + numOps);
        writer.println("Target throughput: " + throughput + " op/sec");
        double actualThroughput = 0;
        for (BenchmarkTask task : benchmarkTasks) {
            actualThroughput += task.getActualThroughput();
        }
        writer.println("Actual throughput: " + actualThroughput + " op/sec");
        writer.println();
        
        for (Pair<String, Double> operationPair : getAvailableOperations()) {
            exportOperationResults(writer, operationPair.getFirst(), outputFolder);
        }
        writer.close();
    }
    
    private void exportOperationResults(PrintWriter writer, String operation, File outputFolder) {
        long sum = 0;
        long min = Long.MAX_VALUE;
        long max = -1;
        long count = 0;
        for (BenchmarkTask task : benchmarkTasks) {
            for (TimeMeasurment tm : task.getLatencies().get(operation)) {
               long latency = tm.getLatency();
               count++;
               sum += latency;
               if (latency < min)
                   min = latency;
               if (latency > max)
                   max = latency;
            }
        }
        
        if (count != 0) {
            writer.println("# " + operation + " latencies");
            writer.println(String.format("%s.ops=%d", operation, count));
            writer.println(String.format("%s.avg=%.4f ms", operation, sum / 1000000.0 / count));
            writer.println(String.format("%s.min=%.4f ms", operation, min / 1000000.0));
            writer.println(String.format("%s.max=%.4f ms", operation, max / 1000000.0));
            writer.println();
            
            try {
                final File tsFile = new File(outputFolder, operation + ".txt");
                for (BenchmarkTask task : benchmarkTasks) {
                    ExportTools.exportTimeSeries(task.getLatencies().get(operation), tsFile);
                }
            } catch (IOException e) {
                logger.error(e);
            }
        }
    }
    
    protected abstract class BenchmarkTask implements MultiThreadedBenchmark.BenchmarkTask<ClientType> {
        protected final Logger logger; 
        protected final int taskNr;
        private final int nrOps;
        private final double timeBeetweenOps;
        private final EnumeratedDistribution<String> opGenerator;
        private final StopWatch stopWatch;
        protected KeyValueStoreClient client;
        private final Map<String, List<TimeMeasurment>> latencies;
        private volatile double actualThroughput;

        public BenchmarkTask() {
            this.taskNr = taskCount.incrementAndGet();
            this.logger = Logger.getLogger(this.getClass().getSimpleName() + "-" + taskNr);
            this.nrOps = numOps / nrThreads;
            final int localThroughput = throughput / nrThreads;
            this.timeBeetweenOps = 1000.0 / localThroughput;
            logger.info("Target th: " + localThroughput + ", ms/op = " + timeBeetweenOps);
            this.stopWatch = new StopWatch();
            this.opGenerator = new EnumeratedDistribution<>(getAvailableOperations());
            this.latencies = new HashMap<>();
            for (Pair<String, Double> opPair : getAvailableOperations()) {
                latencies.put(opPair.getFirst(), new ArrayList<TimeMeasurment>());
            }
        }

        public abstract void doOperation(String op) throws ClientException;

        public void init(KeyValueStoreClient client) {
            this.client = client;
            
            try {
                client.connect();
                logger.info("Database client connected.");
            } catch (ClientException e) {
                logger.error("Failed to connect to the database");
            }
        }

        @Override
        public void run() {
            final long startTime = System.currentTimeMillis();
            for (int i = 1; i <= nrOps; ++i) {
                final long time = System.currentTimeMillis();
                try {
                    final String operation = opGenerator.sample();
                    stopWatch.start();
                    doOperation(operation);
                    final long latency = stopWatch.stop();
                    latencies.get(operation).add(new TimeMeasurment(time, latency));
                } catch (ClientException e) {
                    logger.error("Error when executing operation: ", e);
                }
                
                try {
                    while (startTime + i * timeBeetweenOps > System.currentTimeMillis()) {
                        Thread.sleep(1);
                    }
                } catch (InterruptedException e) {}
            }
            final long totalTime = System.currentTimeMillis() - startTime;
            actualThroughput = 1000.0 * nrOps / totalTime;
            logger.info(String.format("Completed %s operations in %s ms. Actual throughput: %s ops/sec", nrOps, totalTime, actualThroughput));    
        }
        
        @Override
        public void cleanUp() {
            logger.info("Disconnecting...");
            try {
                client.disconnect();
            } catch (ClientException e) {
                logger.error("Disconnect failed: ", e);
            }
        }
        
        public Map<String, List<TimeMeasurment>> getLatencies() {
            return latencies;
        }
        
        public double getActualThroughput() {
            return actualThroughput;
        }

    }

}
