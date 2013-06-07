package com.github.bluetiger9.nosql.benchmarking.benchmarks;

import com.github.bluetiger9.nosql.benchmarking.clients.ClientFactory;
import com.github.bluetiger9.nosql.benchmarking.clients.DatabaseClient;

public interface Benchmark<ClientType extends DatabaseClient> {
    
    void run(ClientFactory<? extends ClientType> clientFactory);    
}
