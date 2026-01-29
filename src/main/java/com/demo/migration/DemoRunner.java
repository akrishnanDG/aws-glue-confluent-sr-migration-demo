package com.demo.migration;

import com.demo.migration.config.KafkaConfig;
import com.demo.migration.consumer.PaymentAnalyticsConsumer;
import com.demo.migration.producer.ConfluentPaymentProducer;
import com.demo.migration.producer.GluePaymentProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Demo Runner for AWS Glue to Confluent Schema Registry Migration.
 * 
 * Uses AWS Glue's secondary.deserializer to read from both SRs seamlessly.
 */
public class DemoRunner {
    
    private static final Logger logger = LoggerFactory.getLogger(DemoRunner.class);
    private static final AtomicBoolean running = new AtomicBoolean(true);
    
    public static void main(String[] args) throws Exception {
        printBanner();
        
        String mode = args.length > 0 ? args[0] : "full";
        
        switch (mode.toLowerCase()) {
            case "producer-glue" -> runGlueProducerOnly(args);
            case "producer-confluent" -> runConfluentProducerOnly(args);
            case "consumer" -> runConsumerOnly();
            default -> runFullDemo(args);
        }
    }
    
    private static void runFullDemo(String[] args) throws Exception {
        int messagesPerProducer = args.length > 1 ? Integer.parseInt(args[1]) : 5;
        
        KafkaConfig.printConfig();
        System.out.println("\nMessages per producer: " + messagesPerProducer);
        System.out.println("Interval: 2 seconds between each message\n");
        
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch consumerReady = new CountDownLatch(1);
        
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            running.set(false);
            executor.shutdownNow();
        }));
        
        try {
            // Start consumer in background
            executor.submit(() -> {
                try (PaymentAnalyticsConsumer consumer = new PaymentAnalyticsConsumer()) {
                    consumerReady.countDown();
                    consumer.consumeMessages(messagesPerProducer * 2, 180_000);
                } catch (Exception e) {
                    logger.error("Consumer error", e);
                }
            });
            
            consumerReady.await(10, TimeUnit.SECONDS);
            Thread.sleep(3000); // Give consumer time to initialize
            
            // Run producers SEQUENTIALLY - alternating between Glue and Confluent
            try (GluePaymentProducer glueProducer = new GluePaymentProducer();
                 ConfluentPaymentProducer confluentProducer = new ConfluentPaymentProducer()) {
                
                for (int i = 0; i < messagesPerProducer && running.get(); i++) {
                    // Send from Glue producer (Web POS)
                    glueProducer.sendPayment();
                    Thread.sleep(2000); // Wait for consumer to process
                    
                    // Send from Confluent producer (Mobile POS)
                    confluentProducer.sendPayment();
                    Thread.sleep(2000); // Wait for consumer to process
                }
                
                glueProducer.flush();
                confluentProducer.flush();
            }
            
            Thread.sleep(3000); // Give consumer time to finish
            running.set(false);
            
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
            
            printCompletionBanner();
            
        } finally {
            executor.shutdownNow();
        }
    }
    
    private static void runGlueProducerOnly(String[] args) throws Exception {
        KafkaConfig.printConfig();
        int count = args.length > 1 ? Integer.parseInt(args[1]) : 10;
        
        try (GluePaymentProducer producer = new GluePaymentProducer()) {
            for (int i = 0; i < count; i++) {
                producer.sendPayment();
                Thread.sleep(1000);
            }
            producer.flush();
        }
    }
    
    private static void runConfluentProducerOnly(String[] args) throws Exception {
        KafkaConfig.printConfig();
        int count = args.length > 1 ? Integer.parseInt(args[1]) : 10;
        
        try (ConfluentPaymentProducer producer = new ConfluentPaymentProducer()) {
            for (int i = 0; i < count; i++) {
                producer.sendPayment();
                Thread.sleep(1000);
            }
            producer.flush();
        }
    }
    
    private static void runConsumerOnly() {
        KafkaConfig.printConfig();
        
        try (PaymentAnalyticsConsumer consumer = new PaymentAnalyticsConsumer()) {
            Runtime.getRuntime().addShutdownHook(new Thread(consumer::stop));
            consumer.start();
        }
    }
    
    private static void printBanner() {
        System.out.println("""
            
            ========================================================================
                   AWS GLUE -> CONFLUENT SCHEMA REGISTRY MIGRATION DEMO
            ========================================================================
            
            Consumer uses AWS Glue's secondary.deserializer for seamless migration
            
                +----------------+                                    
                |   Web POS      |---> AWS Glue SR ----+              
                |   (Legacy)     |                     |   +----------------+
                +----------------+                     +-->|  payments      |
                +----------------+                     |   |  topic         |
                |  Mobile POS    |---> Confluent SR ---+   +-------+--------+
                |    (New)       |                             |
                +----------------+                             v
                                                 +-------------------------+
                                                 | Consumer                |
                                                 | PRIMARY:   AWS Glue SR  |
                                                 | SECONDARY: Confluent SR |
                                                 +-------------------------+
            
            ========================================================================
            """);
    }
    
    private static void printCompletionBanner() {
        System.out.println("""
            
            ========================================================================
                              DEMO COMPLETED SUCCESSFULLY
            ========================================================================
            
            Key Takeaways:
            
              * secondary.deserializer enables reading from BOTH schema registries
              * Magic byte detection: Glue=0x03, Confluent=0x00  
              * Minimal downtime - consumers need one-time upgrade to add
                Confluent SR as secondary deserializer
              * Producers can be migrated gradually to Confluent SR
            
            ========================================================================
            """);
    }
}
