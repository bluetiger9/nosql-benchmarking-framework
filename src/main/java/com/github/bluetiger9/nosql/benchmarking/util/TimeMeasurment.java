package com.github.bluetiger9.nosql.benchmarking.util;

public class TimeMeasurment {
    /** Measurment time in milliseconds */
    private final long time;
    
    /** Latency in nanoseconds */
    private final long latency;

    public TimeMeasurment(long time, long latency) {
        this.time = time;
        this.latency = latency;
    }
    
    public long getTime() {
        return time;
    }
    
    public long getLatency() {
        return latency;
    }
    
    @Override
    public String toString() {
        return time + " " + latency;
    }
}
