package com.github.bluetiger9.nosql.benchmarking;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class Util {
    
    public static Properties loadProperties(String fileName) throws IOException {
        final Properties properties = new Properties();
        properties.load(new FileInputStream(fileName));
        return properties;
    }
    
    public static String getMandatoryProperty(Properties properties, String property) {
        return getMandatoryProperty(properties, property, 
                String.format("The mandatory property %s is not present.", property));
    }
    
    public static String getMandatoryProperty(Properties properties, String property, String message) {
        final String value = properties.getProperty(property);
        if (value == null) {
            throw new IllegalArgumentException(message);
        }
        return value;
    }
    
}
