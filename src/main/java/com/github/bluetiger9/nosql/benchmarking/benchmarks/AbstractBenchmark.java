package com.github.bluetiger9.nosql.benchmarking.benchmarks;

import java.util.Properties;

import com.github.bluetiger9.nosql.benchmarking.Component;
import com.github.bluetiger9.nosql.benchmarking.clients.ClientFactory;
import com.github.bluetiger9.nosql.benchmarking.clients.DatabaseClient;

public abstract class AbstractBenchmark<ClientType extends DatabaseClient> extends Component implements
        Benchmark<ClientType> {

    protected ClientFactory<? extends ClientType> clientFactory;
    
    public AbstractBenchmark(Properties props) {
        super(props);
    }
    
    public void initBenchmark(ClientFactory<? extends ClientType> clientFactory) {
        this.clientFactory = clientFactory;
    }
}
