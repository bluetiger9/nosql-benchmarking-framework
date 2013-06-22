package com.github.bluetiger9.nosql.benchmarking.util;

/**
 * Nanosecond resolution stop-watch.
 */
public final class StopWatch {
    private long time;
    
    public StopWatch() {}
    
    public void start() {
        time = System.nanoTime();        
    }
    
    public long stop() {
        return System.nanoTime() - time;
    }
    

}
