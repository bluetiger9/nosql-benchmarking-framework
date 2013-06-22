package com.github.bluetiger9.nosql.benchmarking.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.lang.RandomStringUtils;

public class KeyGenerator {
    final List<String> keySet;
    final Random random;
    
    public KeyGenerator() {
        this.keySet = new ArrayList<>();
        this.random = new Random();
    }
    
    public void add(String key) {
        keySet.add(key);
    }
    
    public String getRandomKey() {
        return keySet.get(random.nextInt(keySet.size())); 
    }
    
    public List<String> getAllKeys() {
        return keySet;
    }
    
    public static String newRandomKey(int length) {
        return RandomStringUtils.randomAlphanumeric(length);        
    }
}
