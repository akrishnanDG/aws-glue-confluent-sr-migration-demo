package com.demo.migration.consumer;

import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.demo.migration.config.KafkaConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.glue.model.DataFormat;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Payment Analytics Consumer - Demonstrates AWS Glue to Confluent SR Migration
 * 
 * This consumer uses AWS Glue SR as the PRIMARY deserializer with Confluent SR 
 * configured as the SECONDARY deserializer via the 'secondary.deserializer' feature.
 * 
 * MIGRATION STRATEGY:
 * -------------------
 * This is Phase 2 of a 4-phase migration approach:
 * 
 * Phase 1: Pre-migration - All producers and consumers use AWS Glue SR
 * Phase 2: Dual-read (THIS DEMO) - Consumer reads from both SRs via secondary.deserializer
 * Phase 3: Producer migration - Gradually move producers to Confluent SR
 * Phase 4: Full migration - Remove AWS Glue SR, use Confluent SR directly
 * 
 * The secondary.deserializer feature detects the wire format via magic bytes:
 *   - 0x03 = AWS Glue SR message → uses primary (Glue) deserializer
 *   - 0x00 = Confluent SR message → uses secondary (Confluent) deserializer
 */
public class PaymentAnalyticsConsumer implements AutoCloseable {
    
    private static final Logger logger = LoggerFactory.getLogger(PaymentAnalyticsConsumer.class);
    
    private final KafkaConsumer<String, GenericRecord> consumer;
    private final String topic;
    private final AtomicBoolean running = new AtomicBoolean(false);
    
    // Analytics tracking
    private final AtomicInteger totalMessages = new AtomicInteger(0);
    private final AtomicInteger glueMessages = new AtomicInteger(0);
    private final AtomicInteger confluentMessages = new AtomicInteger(0);
    private final Map<String, Double> revenueByChannel = Collections.synchronizedMap(new HashMap<>());
    private final Map<String, Integer> paymentsByMethod = Collections.synchronizedMap(new HashMap<>());
    
    private static final DateTimeFormatter TIME_FMT = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
    
    public PaymentAnalyticsConsumer() {
        Properties appProps = KafkaConfig.loadProperties("consumer.properties");
        Properties secrets = KafkaConfig.getSecrets();
        
        // Set AWS credentials as system properties for AWS SDK
        setAwsCredentials(secrets);
        
        this.topic = appProps.getProperty("topic");
        this.consumer = new KafkaConsumer<>(buildConsumerConfig(appProps, secrets));
        
        logger.info("Payment Analytics Consumer initialized");
        logger.info("   PRIMARY: AWS Glue SR ({})", secrets.getProperty("aws.glue.registry.name"));
        logger.info("   SECONDARY: Confluent SR ({})", secrets.getProperty("confluent.sr.url"));
    }
    
    private void setAwsCredentials(Properties secrets) {
        String accessKey = secrets.getProperty("aws.access.key.id", "");
        String secretKey = secrets.getProperty("aws.secret.access.key", "");
        
        if (!accessKey.isEmpty() && !secretKey.isEmpty()) {
            System.setProperty("aws.accessKeyId", accessKey);
            System.setProperty("aws.secretAccessKey", secretKey);
        }
    }
    
    /**
     * Builds consumer configuration with DUAL DESERIALIZER support for migration.
     * 
     * The configuration is organized into sections:
     * 1. Kafka cluster connection (existing)
     * 2. Consumer settings (existing)
     * 3. AWS Glue SR - PRIMARY deserializer (existing)
     * 4. Confluent SR - SECONDARY deserializer (NEW FOR MIGRATION)
     */
    private Properties buildConsumerConfig(Properties appProps, Properties secrets) {
        Properties config = new Properties();
        
        // =========================================================================
        // SECTION 1: KAFKA CLUSTER CONNECTION (existing configuration)
        // =========================================================================
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, secrets.getProperty("kafka.bootstrap.servers"));
        config.put("security.protocol", "SASL_SSL");
        config.put("sasl.mechanism", "PLAIN");
        config.put("sasl.jaas.config", 
            "org.apache.kafka.common.security.plain.PlainLoginModule required " +
            "username=\"" + secrets.getProperty("kafka.api.key") + "\" " +
            "password=\"" + secrets.getProperty("kafka.api.secret") + "\";");
        
        // =========================================================================
        // SECTION 2: CONSUMER SETTINGS (existing configuration)
        // =========================================================================
        config.put(ConsumerConfig.CLIENT_ID_CONFIG, appProps.getProperty("client.id"));
        config.put(ConsumerConfig.GROUP_ID_CONFIG, appProps.getProperty("group.id"));
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, appProps.getProperty("auto.offset.reset"));
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, appProps.getProperty("enable.auto.commit"));
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        
        // =========================================================================
        // SECTION 3: AWS GLUE SCHEMA REGISTRY - PRIMARY DESERIALIZER
        // (existing configuration - this was your original setup)
        // =========================================================================
        // Use our wrapper deserializer to capture schema IDs for demo purposes
        // In production, use: GlueSchemaRegistryKafkaDeserializer.class.getName()
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SchemaIdCapturingDeserializer.class.getName());
        
        // AWS Glue SR settings (existing)
        config.put(AWSSchemaRegistryConstants.AWS_REGION, secrets.getProperty("aws.region"));
        config.put(AWSSchemaRegistryConstants.REGISTRY_NAME, secrets.getProperty("aws.glue.registry.name"));
        config.put(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE, appProps.getProperty("avroRecordType"));
        config.put(AWSSchemaRegistryConstants.DATA_FORMAT, DataFormat.AVRO.name());
        
        // =========================================================================
        // SECTION 4: CONFLUENT SCHEMA REGISTRY - SECONDARY DESERIALIZER
        // *** THIS IS THE KEY CONFIGURATION FOR MIGRATION ***
        // 
        // Add these properties to enable dual-read from both schema registries.
        // The AWS Glue deserializer will automatically delegate to this secondary
        // deserializer when it detects a Confluent wire format (magic byte 0x00).
        // =========================================================================
        
        // 4a. Enable secondary deserializer - point to Confluent's KafkaAvroDeserializer
        // This tells AWS Glue SR to use this deserializer for non-Glue messages
        config.put(AWSSchemaRegistryConstants.SECONDARY_DESERIALIZER, 
            KafkaAvroDeserializer.class.getName());
        
        // 4b. Confluent Schema Registry URL
        // The endpoint for your Confluent Cloud Schema Registry
        config.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, 
            secrets.getProperty("confluent.sr.url"));
        
        // 4c. Confluent SR Authentication
        // Required for Confluent Cloud - uses HTTP Basic Auth with API key/secret
        config.put("basic.auth.credentials.source", "USER_INFO");
        config.put("basic.auth.user.info", 
            secrets.getProperty("confluent.sr.api.key") + ":" + secrets.getProperty("confluent.sr.api.secret"));
        
        // 4d. Avro reader configuration for Confluent deserializer
        // Set to false to use GenericRecord (compatible with Glue's output)
        config.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, 
            Boolean.parseBoolean(appProps.getProperty("specific.avro.reader")));
        
        return config;
    }
    
    public void start() {
        consumer.subscribe(Collections.singletonList(topic));
        running.set(true);
        
        logger.info("Consumer started. Listening on topic: {}\n", topic);
        
        while (running.get()) {
            ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, GenericRecord> record : records) {
                processRecord(record);
            }
        }
    }
    
    public void consumeMessages(int maxMessages, long timeoutMs) {
        consumer.subscribe(Collections.singletonList(topic));
        running.set(true);
        
        logger.info("Consumer started. Waiting for {} messages...\n", maxMessages);
        long startTime = System.currentTimeMillis();
        
        while (running.get() && totalMessages.get() < maxMessages && 
               (System.currentTimeMillis() - startTime) < timeoutMs) {
            
            ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, GenericRecord> record : records) {
                processRecord(record);
                if (totalMessages.get() >= maxMessages) break;
            }
        }
    }
    
    private void processRecord(ConsumerRecord<String, GenericRecord> record) {
        GenericRecord payment = record.value();
        if (payment == null) {
            logger.warn("Null record at offset {} - skipping", record.offset());
            return;
        }
        
        // Get schema ID captured during deserialization (extracted from wire format)
        SchemaIdCapturingDeserializer.SchemaIdInfo schemaIdInfo = SchemaIdCapturingDeserializer.getLastSchemaId();
        
        // Determine which SR was used based on magic byte detection
        String registryType = schemaIdInfo != null ? schemaIdInfo.registryType : "UNKNOWN";
        String schemaId = schemaIdInfo != null ? schemaIdInfo.schemaId : "N/A";
        
        // Update counters
        if ("GLUE".equals(registryType)) {
            glueMessages.incrementAndGet();
        } else {
            confluentMessages.incrementAndGet();
        }
        totalMessages.incrementAndGet();
        
        // Extract payment fields
        String txnId = payment.get("transactionId").toString();
        String customerId = payment.get("customerId").toString();
        double amount = (Double) payment.get("amount");
        String currency = payment.get("currency").toString();
        String posChannel = payment.get("posChannel").toString();
        String paymentMethod = payment.get("paymentMethod").toString();
        String paymentStatus = payment.get("paymentStatus").toString();
        String merchantName = payment.get("merchantName").toString();
        long timestamp = (Long) payment.get("timestamp");
        
        // Update analytics
        revenueByChannel.merge(posChannel, amount, Double::sum);
        paymentsByMethod.merge(paymentMethod, 1, Integer::sum);
        
        LocalDateTime dateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault());
        
        // Print payment details using simple formatting
        printPaymentDetails(registryType, schemaId, txnId, customerId, merchantName, 
            amount, currency, paymentMethod, paymentStatus, posChannel, dateTime, record);
    }
    
    /**
     * Prints payment details in a clean, aligned format.
     */
    private void printPaymentDetails(String registryType, String schemaId, String txnId,
            String customerId, String merchantName, double amount, String currency,
            String paymentMethod, String paymentStatus, String posChannel,
            LocalDateTime dateTime, ConsumerRecord<String, GenericRecord> record) {
        
        String srLabel = "GLUE".equals(registryType) ? "AWS GLUE SR" : "CONFLUENT SR";
        String source = "GLUE".equals(registryType) ? "Web POS (Legacy)" : "Mobile POS (New)";
        
        System.out.println();
        System.out.println("    " + "+".repeat(64));
        System.out.printf("    |  CONSUMER RECEIVED - Deserialized via %-21s |%n", srLabel);
        System.out.println("    " + "+".repeat(64));
        System.out.printf("    |  Source       : %-44s |%n", source);
        System.out.printf("    |  Transaction  : %-44s |%n", txnId);
        System.out.printf("    |  Customer     : %-44s |%n", customerId);
        System.out.printf("    |  Merchant     : %-44s |%n", truncate(merchantName, 44));
        System.out.printf("    |  Amount       : $%-43.2f |%n", amount);
        System.out.printf("    |  Method       : %-44s |%n", paymentMethod);
        System.out.printf("    |  Channel      : %-44s |%n", posChannel);
        System.out.println("    " + "|" + "-".repeat(62) + "|");
        System.out.printf("    |  Schema ID    : %-44s |%n", truncate(schemaId, 44));
        System.out.printf("    |  Registry     : %-44s |%n", srLabel);
        System.out.println("    " + "+".repeat(64));
    }
    
    private String truncate(String str, int max) {
        return str.length() <= max ? str : str.substring(0, max - 3) + "...";
    }
    
    public void printSummary() {
        System.out.println();
        System.out.println("=".repeat(72));
        System.out.println("                      ANALYTICS SUMMARY");
        System.out.println("=".repeat(72));
        System.out.printf("  Total Payments: %d%n", totalMessages.get());
        System.out.println();
        System.out.println("  By Schema Registry (Deserializer Used):");
        System.out.printf("    AWS Glue SR (Web POS)      : %d%n", glueMessages.get());
        System.out.printf("    Confluent SR (Mobile POS)  : %d%n", confluentMessages.get());
        System.out.println();
        System.out.println("  Revenue by Channel:");
        revenueByChannel.forEach((ch, rev) -> 
            System.out.printf("    %-15s : $%.2f%n", ch, rev));
        System.out.println();
        System.out.println("  Payments by Method:");
        paymentsByMethod.forEach((m, c) -> 
            System.out.printf("    %-20s : %d%n", m, c));
        System.out.println("=".repeat(72));
    }
    
    public void stop() { running.set(false); }
    public int getTotalMessages() { return totalMessages.get(); }
    
    @Override
    public void close() {
        consumer.close();
    }
    
    public static void main(String[] args) {
        KafkaConfig.printConfig();
        
        try (PaymentAnalyticsConsumer consumer = new PaymentAnalyticsConsumer()) {
            Runtime.getRuntime().addShutdownHook(new Thread(consumer::stop));
            consumer.start();
        }
    }
}
