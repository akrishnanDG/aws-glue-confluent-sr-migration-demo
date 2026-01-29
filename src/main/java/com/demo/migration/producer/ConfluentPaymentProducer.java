package com.demo.migration.producer;

import com.demo.migration.config.KafkaConfig;
import com.demo.migration.model.Payment;
import com.demo.migration.model.PaymentMethod;
import com.demo.migration.model.PaymentStatus;
import com.demo.migration.model.PosChannel;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import net.datafaker.Faker;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Mobile POS Producer (New System) - Uses Confluent Schema Registry
 */
public class ConfluentPaymentProducer implements AutoCloseable {
    
    private static final Logger logger = LoggerFactory.getLogger(ConfluentPaymentProducer.class);
    
    private final KafkaProducer<String, Payment> producer;
    private final String topic;
    private final Faker faker = new Faker();
    private final AtomicInteger messageCount = new AtomicInteger(0);
    
    public ConfluentPaymentProducer() {
        Properties appProps = KafkaConfig.loadProperties("confluent-producer.properties");
        Properties secrets = KafkaConfig.getSecrets();
        
        this.topic = appProps.getProperty("topic");
        this.producer = new KafkaProducer<>(buildProducerConfig(appProps, secrets));
        
        logger.info("📱 Mobile POS Producer initialized");
        logger.info("   Schema Registry: Confluent SR");
        logger.info("   URL: {}", secrets.getProperty("confluent.sr.url"));
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
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        
        // Confluent SR (from secrets + app config)
        config.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, secrets.getProperty("confluent.sr.url"));
        config.put("basic.auth.credentials.source", "USER_INFO");
        config.put("basic.auth.user.info", 
            secrets.getProperty("confluent.sr.api.key") + ":" + secrets.getProperty("confluent.sr.api.secret"));
        config.put(KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS, 
            Boolean.parseBoolean(appProps.getProperty("auto.register.schemas")));
        // When auto.register is false, use the pre-registered schema
        config.put(KafkaAvroSerializerConfig.USE_LATEST_VERSION, true);
        
        return config;
    }
    
    public void sendPayment() throws ExecutionException, InterruptedException {
        Payment payment = generatePayment();
        
        ProducerRecord<String, Payment> record = new ProducerRecord<>(topic, payment.getTransactionId(), payment);
        RecordMetadata metadata = producer.send(record).get();
        
        messageCount.incrementAndGet();
        System.out.println();
        System.out.println("    ---- MESSAGE SENT ----");
        System.out.println("    SERIALIZER : CONFLUENT SR");
        System.out.printf("    Transaction: %s%n", payment.getTransactionId());
        System.out.printf("    Amount     : $%.2f%n", payment.getAmount());
        System.out.printf("    Merchant   : %s%n", payment.getMerchantName());
    }
    
    private Payment generatePayment() {
        String transactionId = "MOB-" + faker.number().digits(8).toUpperCase();
        double amount = faker.number().randomDouble(2, 5, 150);
        
        PaymentMethod[] methods = {PaymentMethod.DIGITAL_WALLET, PaymentMethod.DIGITAL_WALLET,
                                   PaymentMethod.CREDIT_CARD, PaymentMethod.DEBIT_CARD};
        PaymentMethod method = methods[faker.random().nextInt(methods.length)];
        
        String[] businessTypes = {"Coffee Shop", "Food Delivery", "Ride Share", "Quick Service"};
        String merchantName = faker.company().name() + " " + businessTypes[faker.random().nextInt(businessTypes.length)];
        
        Map<String, String> metadata = new HashMap<>();
        metadata.put("app_version", faker.app().version());
        metadata.put("device_os", faker.options().option("iOS 17", "Android 14"));
        metadata.put("device_model", faker.options().option("iPhone 15 Pro", "Pixel 8", "Galaxy S24"));
        metadata.put("location", faker.address().city() + ", " + faker.address().stateAbbr());
        
        return Payment.newBuilder()
            .setTransactionId(transactionId)
            .setOrderId("ORD-" + faker.number().digits(8))
            .setCustomerId("CUST-" + faker.number().digits(6))
            .setAmount(amount)
            .setCurrency("USD")
            .setPaymentMethod(method)
            .setPaymentStatus(PaymentStatus.CAPTURED)
            .setPosChannel(PosChannel.MOBILE_APP)
            .setMerchantId("MRK-" + faker.number().digits(6))
            .setMerchantName(merchantName)
            .setCardLastFour(method == PaymentMethod.DIGITAL_WALLET ? null : faker.number().digits(4))
            .setTimestamp(System.currentTimeMillis())
            .setMetadata(metadata)
            .build();
    }
    
    public int getMessageCount() { return messageCount.get(); }
    public void flush() { producer.flush(); }
    
    @Override
    public void close() {
        logger.info("Closing Mobile POS Producer. Messages sent: {}", messageCount.get());
        producer.close();
    }
    
    public static void main(String[] args) throws Exception {
        KafkaConfig.printConfig();
        int count = args.length > 0 ? Integer.parseInt(args[0]) : 10;
        
        try (ConfluentPaymentProducer producer = new ConfluentPaymentProducer()) {
            for (int i = 0; i < count; i++) {
                producer.sendPayment();
                Thread.sleep(1000);
            }
            producer.flush();
        }
    }
}
