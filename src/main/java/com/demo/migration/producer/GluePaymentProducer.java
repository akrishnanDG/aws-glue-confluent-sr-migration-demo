package com.demo.migration.producer;

import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistryKafkaSerializer;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.demo.migration.config.KafkaConfig;
import com.demo.migration.model.Payment;
import com.demo.migration.model.PaymentMethod;
import com.demo.migration.model.PaymentStatus;
import com.demo.migration.model.PosChannel;
import net.datafaker.Faker;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.glue.model.DataFormat;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Web POS Producer (Legacy System) - Uses AWS Glue Schema Registry
 */
public class GluePaymentProducer implements AutoCloseable {
    
    private static final Logger logger = LoggerFactory.getLogger(GluePaymentProducer.class);
    
    private final KafkaProducer<String, Payment> producer;
    private final String topic;
    private final Faker faker = new Faker();
    private final AtomicInteger messageCount = new AtomicInteger(0);
    
    public GluePaymentProducer() {
        Properties appProps = KafkaConfig.loadProperties("glue-producer.properties");
        Properties secrets = KafkaConfig.getSecrets();
        
        // Set AWS credentials as system properties for AWS SDK
        setAwsCredentials(secrets);
        
        this.topic = appProps.getProperty("topic");
        this.producer = new KafkaProducer<>(buildProducerConfig(appProps, secrets));
        
        logger.info("🌐 Web POS Producer initialized");
        logger.info("   Schema Registry: AWS Glue SR");
        logger.info("   Registry: {}", secrets.getProperty("aws.glue.registry.name"));
    }
    
    private void setAwsCredentials(Properties secrets) {
        String accessKey = secrets.getProperty("aws.access.key.id", "");
        String secretKey = secrets.getProperty("aws.secret.access.key", "");
        
        if (!accessKey.isEmpty() && !secretKey.isEmpty()) {
            System.setProperty("aws.accessKeyId", accessKey);
            System.setProperty("aws.secretAccessKey", secretKey);
        }
    }
    
    private Properties buildProducerConfig(Properties appProps, Properties secrets) {
        Properties config = new Properties();
        
        // Kafka cluster (from secrets)
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, secrets.getProperty("kafka.bootstrap.servers"));
        config.put("security.protocol", "SASL_SSL");
        config.put("sasl.mechanism", "PLAIN");
        config.put("sasl.jaas.config", 
            "org.apache.kafka.common.security.plain.PlainLoginModule required " +
            "username=\"" + secrets.getProperty("kafka.api.key") + "\" " +
            "password=\"" + secrets.getProperty("kafka.api.secret") + "\";");
        
        // Producer settings (from app config)
        config.put(ProducerConfig.CLIENT_ID_CONFIG, appProps.getProperty("client.id"));
        config.put(ProducerConfig.ACKS_CONFIG, appProps.getProperty("acks"));
        config.put(ProducerConfig.RETRIES_CONFIG, Integer.parseInt(appProps.getProperty("retries")));
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GlueSchemaRegistryKafkaSerializer.class.getName());
        
        // AWS Glue SR (from secrets + app config)
        config.put(AWSSchemaRegistryConstants.AWS_REGION, secrets.getProperty("aws.region"));
        config.put(AWSSchemaRegistryConstants.REGISTRY_NAME, secrets.getProperty("aws.glue.registry.name"));
        config.put(AWSSchemaRegistryConstants.SCHEMA_NAME, appProps.getProperty("schemaName"));
        config.put(AWSSchemaRegistryConstants.DATA_FORMAT, DataFormat.AVRO.name());
        config.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, appProps.getProperty("schemaAutoRegistrationEnabled"));
        config.put(AWSSchemaRegistryConstants.COMPATIBILITY_SETTING, appProps.getProperty("compatibility"));
        
        return config;
    }
    
    public void sendPayment() throws ExecutionException, InterruptedException {
        Payment payment = generatePayment();
        
        ProducerRecord<String, Payment> record = new ProducerRecord<>(topic, payment.getTransactionId(), payment);
        RecordMetadata metadata = producer.send(record).get();
        
        messageCount.incrementAndGet();
        System.out.println();
        System.out.println("    ---- MESSAGE SENT ----");
        System.out.println("    SERIALIZER : AWS GLUE SR");
        System.out.printf("    Transaction: %s%n", payment.getTransactionId());
        System.out.printf("    Amount     : $%.2f%n", payment.getAmount());
        System.out.printf("    Merchant   : %s%n", payment.getMerchantName());
    }
    
    private Payment generatePayment() {
        String transactionId = "WEB-" + faker.number().digits(8).toUpperCase();
        double amount = faker.number().randomDouble(2, 25, 500);
        
        PaymentMethod[] methods = {PaymentMethod.CREDIT_CARD, PaymentMethod.CREDIT_CARD, 
                                   PaymentMethod.DIGITAL_WALLET, PaymentMethod.DEBIT_CARD};
        
        Map<String, String> metadata = new HashMap<>();
        metadata.put("browser", faker.options().option("Chrome", "Safari", "Firefox", "Edge"));
        metadata.put("ip_address", faker.internet().ipV4Address());
        metadata.put("country", faker.address().countryCode());
        
        return Payment.newBuilder()
            .setTransactionId(transactionId)
            .setOrderId("ORD-" + faker.number().digits(8))
            .setCustomerId("CUST-" + faker.number().digits(6))
            .setAmount(amount)
            .setCurrency("USD")
            .setPaymentMethod(methods[faker.random().nextInt(methods.length)])
            .setPaymentStatus(PaymentStatus.CAPTURED)
            .setPosChannel(PosChannel.WEB)
            .setMerchantId("MRK-" + faker.number().digits(6))
            .setMerchantName(faker.company().name())
            .setCardLastFour(faker.number().digits(4))
            .setTimestamp(System.currentTimeMillis())
            .setMetadata(metadata)
            .build();
    }
    
    public int getMessageCount() { return messageCount.get(); }
    public void flush() { producer.flush(); }
    
    @Override
    public void close() {
        logger.info("Closing Web POS Producer. Messages sent: {}", messageCount.get());
        producer.close();
    }
    
    public static void main(String[] args) throws Exception {
        KafkaConfig.printConfig();
        int count = args.length > 0 ? Integer.parseInt(args[0]) : 10;
        
        try (GluePaymentProducer producer = new GluePaymentProducer()) {
            for (int i = 0; i < count; i++) {
                producer.sendPayment();
                Thread.sleep(1000);
            }
            producer.flush();
        }
    }
}
