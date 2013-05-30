package com.github.bluetiger9.nosql.benchmarking;

import java.lang.reflect.InvocationTargetException;
import java.util.Properties;

import org.apache.log4j.Logger;

public abstract class ComponentFactory {
    private static final Logger LOGGER = Logger.getLogger(Main.class);

    private ComponentFactory() {
    }

    public static <E extends Component> E constructFromProperties(Class<E> clazz, Properties properties) {
        try {
            return clazz.getConstructor(Properties.class).newInstance(properties);
        } catch (NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException
                | IllegalArgumentException | InvocationTargetException e) {
            LOGGER.error("Unable to construct object.", e);
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    public static <E extends Component, S> S constructFromProperties(Class<S> superClass,
            Class<E> clazz, Properties properties) {
        if (!superClass.isAssignableFrom(clazz)) {
            throw new IllegalArgumentException(String.format("Class %s is not a subtype of %s", clazz.getName(),
                    superClass.getName()));
        }
        return (S) constructFromProperties(clazz, properties);
    }
    
    @SuppressWarnings("unchecked")
    public static <E> Class<? extends E> loadClass(Class<E> superClass, String className) throws ClassNotFoundException {
        final Class<?> clazz = Class.forName(className);
        if (!superClass.isAssignableFrom(clazz)) {
            throw new IllegalArgumentException(String.format("Class %s is not a subtype of %s", clazz.getName(),
                    superClass.getName()));
        }
        return (Class<? extends E>) clazz;
    }
}