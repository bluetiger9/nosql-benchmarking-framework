package com.github.bluetiger9.nosql.benchmarking.clients;

public interface ClientFactory<ClientType> {
    
    ClientType createClient();
    
}
