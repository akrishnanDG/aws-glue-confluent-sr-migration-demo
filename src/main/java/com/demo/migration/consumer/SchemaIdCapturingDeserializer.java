package com.demo.migration.consumer;

import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryKafkaDeserializer;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;

/**
 * A wrapper deserializer that captures the schema ID from the raw message bytes
 * before delegating to the actual Glue SR deserializer.
 * 
 * This class is used for DEMO PURPOSES to show which schema registry was used
 * for each message. In production, you would use GlueSchemaRegistryKafkaDeserializer directly.
 * 
 * Wire Format Detection:
 * ----------------------
 * The serializers embed schema information in the message bytes using different formats:
 * 
 * CONFLUENT SR Format:
 *   [0x00][4-byte schema ID][avro data]
 *   - Magic byte: 0x00
 *   - Schema ID: 4-byte big-endian integer
 *   - Example: Schema ID 100001
 * 
 * AWS GLUE SR Format:
 *   [0x03][1-byte compression][16-byte UUID][avro data]
 *   - Magic byte: 0x03
 *   - Compression flag: 1 byte (0 = none, 5 = zlib)
 *   - Schema Version ID: 16-byte UUID
 *   - Example: 3fca6cab-f9ef-41f9-82d4-c1403c909013
 */
public class SchemaIdCapturingDeserializer implements Deserializer<Object> {
    
    /** Confluent Schema Registry magic byte */
    private static final byte CONFLUENT_MAGIC_BYTE = 0x00;
    
    /** AWS Glue Schema Registry magic byte */
    private static final byte GLUE_MAGIC_BYTE = 0x03;
    
    /** Delegate to actual Glue deserializer (which handles secondary deserializer internally) */
    private final GlueSchemaRegistryKafkaDeserializer delegate = new GlueSchemaRegistryKafkaDeserializer();
    
    /** Thread-local storage for the last captured schema ID (safe for concurrent consumers) */
    private static final ThreadLocal<SchemaIdInfo> lastSchemaId = new ThreadLocal<>();
    
    /**
     * Contains information about the schema used to serialize a message.
     */
    public static class SchemaIdInfo {
        /** Registry type: "CONFLUENT" or "GLUE" */
        public final String registryType;
        
        /** Schema ID: Integer string for Confluent, UUID string for Glue */
        public final String schemaId;
        
        public SchemaIdInfo(String registryType, String schemaId) {
            this.registryType = registryType;
            this.schemaId = schemaId;
        }
    }
    
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        delegate.configure(configs, isKey);
    }
    
    @Override
    public Object deserialize(String topic, byte[] data) {
        if (data == null || data.length == 0) {
            lastSchemaId.set(null);
            return null;
        }
        
        // Extract schema ID from wire format before deserializing
        extractSchemaId(data);
        
        // Delegate to actual deserializer (handles primary/secondary routing internally)
        return delegate.deserialize(topic, data);
    }
    
    /**
     * Extracts the schema ID from the raw message bytes based on the magic byte.
     * 
     * @param data Raw message bytes
     */
    private void extractSchemaId(byte[] data) {
        byte magicByte = data[0];
        
        if (magicByte == CONFLUENT_MAGIC_BYTE) {
            extractConfluentSchemaId(data);
        } else if (magicByte == GLUE_MAGIC_BYTE) {
            extractGlueSchemaId(data);
        } else {
            lastSchemaId.set(new SchemaIdInfo("UNKNOWN", "magic=0x" + String.format("%02X", magicByte)));
        }
    }
    
    /**
     * Extracts schema ID from Confluent wire format.
     * Format: [0x00][4-byte schema ID][avro data]
     */
    private void extractConfluentSchemaId(byte[] data) {
        if (data.length >= 5) {
            ByteBuffer buffer = ByteBuffer.wrap(data, 1, 4);
            int schemaId = buffer.getInt();
            lastSchemaId.set(new SchemaIdInfo("CONFLUENT", String.valueOf(schemaId)));
        } else {
            lastSchemaId.set(new SchemaIdInfo("CONFLUENT", "invalid"));
        }
    }
    
    /**
     * Extracts schema version ID from AWS Glue wire format.
     * Format: [0x03][1-byte compression][16-byte UUID][avro data]
     */
    private void extractGlueSchemaId(byte[] data) {
        if (data.length >= 18) {
            // Skip magic byte (1) and compression byte (1), read UUID (16 bytes)
            ByteBuffer buffer = ByteBuffer.wrap(data, 2, 16);
            long mostSigBits = buffer.getLong();
            long leastSigBits = buffer.getLong();
            UUID schemaVersionId = new UUID(mostSigBits, leastSigBits);
            lastSchemaId.set(new SchemaIdInfo("GLUE", schemaVersionId.toString()));
        } else {
            lastSchemaId.set(new SchemaIdInfo("GLUE", "invalid"));
        }
    }
    
    @Override
    public void close() {
        delegate.close();
    }
    
    /**
     * Returns the schema ID info from the last deserialized message on this thread.
     * 
     * Call this immediately after deserialize() to get schema info for that message.
     * Thread-safe: each thread maintains its own last schema ID.
     * 
     * @return SchemaIdInfo or null if no message has been deserialized
     */
    public static SchemaIdInfo getLastSchemaId() {
        return lastSchemaId.get();
    }
}
