package com.demo.migration.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Utility to load properties files and merge with secrets.
 */
public class KafkaConfig {
    
    private static final Properties secrets = new Properties();
    
    static {
        try (InputStream input = KafkaConfig.class.getClassLoader()
                .getResourceAsStream("secrets.properties")) {
            if (input != null) {
                secrets.load(input);
            }
        } catch (IOException e) {
            System.err.println("Warning: Could not load secrets.properties");
        }
    }
    
    /**
     * Load app-specific properties and merge with secrets
     */
    public static Properties loadProperties(String filename) {
        Properties props = new Properties();
        
        try (InputStream input = KafkaConfig.class.getClassLoader()
                .getResourceAsStream(filename)) {
            if (input != null) {
                props.load(input);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to load " + filename, e);
        }
        
        return props;
    }
    
    public static Properties getSecrets() {
        return secrets;
    }
    
    public static String getSecret(String key) {
        return secrets.getProperty(key, "");
    }
    
    public static void printConfig() {
        System.out.println("=== Configuration ===");
        System.out.println("Kafka Bootstrap: " + secrets.getProperty("kafka.bootstrap.servers"));
        System.out.println("Confluent SR: " + secrets.getProperty("confluent.sr.url"));
        System.out.println("AWS Region: " + secrets.getProperty("aws.region"));
        System.out.println("Glue Registry: " + secrets.getProperty("aws.glue.registry.name"));
        System.out.println("=====================");
    }
}
