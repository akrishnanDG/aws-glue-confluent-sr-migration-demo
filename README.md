# AWS Glue to Confluent Schema Registry Migration Demo

This project demonstrates migrating from **AWS Glue Schema Registry** to **Confluent Cloud Schema Registry** using AWS Glue's built-in **`secondary.deserializer`** feature.

## Assumptions

This demo assumes:
- **Schemas have already been copied** from AWS Glue SR to Confluent SR (Phase 1 completed)
- Both schema registries contain compatible schemas
- Kafka data (topic messages) has been already migrated to Confluent Cloud topic

## Dependencies

| Library | Version | Purpose |
|---------|---------|---------|
| Java | 17+ | Runtime |
| Apache Kafka Clients | 3.8.0 | Kafka producer/consumer |
| Apache Avro | 1.11.3 | Schema serialization |
| Confluent Kafka Avro Serializer | 7.7.0 | Confluent SR serializer/deserializer |
| Confluent Schema Registry Client | 7.7.0 | Confluent SR client |
| AWS Glue Schema Registry Serde | 1.1.19 | AWS Glue SR serde |
| AWS SDK | 2.21.42 | AWS authentication |
| DataFaker | 2.1.0 | Realistic test data |
| SLF4J + Logback | 2.0.9 / 1.4.14 | Logging |

All dependencies are managed via Maven. See `pom.xml` for details.

## Overview

The demo uses a realistic Point-of-Sale (PoS) payment system scenario with:
- **Legacy Web POS** → Produces payments using AWS Glue SR
- **New Mobile POS** → Produces payments using Confluent SR
- **Payment Analytics Consumer** → Reads from both using `secondary.deserializer`

```
========================================================================
                     MIGRATION ARCHITECTURE
========================================================================

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
```

---

## Full Migration Strategy

The `secondary.deserializer` approach is an **intermediate step** in a full migration. Here's the complete migration plan:

### Phase 1: Pre-Migration (Preparation)
**Goal:** Prepare Confluent SR with schemas before any code changes

**Steps:**
1. Export all schemas from AWS Glue SR
2. Register schemas in Confluent SR with appropriate subject names
3. Verify schema compatibility between both registries

**Important: Subject Naming Strategy Differences**

AWS Glue SR and Confluent SR use **different subject naming conventions**:

| Registry | Key Schema Subject | Value Schema Subject |
|----------|-------------------|---------------------|
| **AWS Glue SR** | Schema name only (e.g., `PaymentKey`) | Schema name only (e.g., `Payment`) |
| **Confluent SR** | `<topic>-key` (e.g., `payments-key`) | `<topic>-value` (e.g., `payments-value`) |

When copying schemas to Confluent SR, you must:
- Register value schemas with subject: `<topic-name>-value`
- Register key schemas with subject: `<topic-name>-key`
- Ensure the schema content (Avro JSON) is identical

**Example:** If your Glue SR has a schema named `Payment` used for topic `payments`:
- In Confluent SR, register it as subject `payments-value`

**Downtime:** None - this is preparation work

### Phase 2: Enable Dual-Read (THIS DEMO)
**Goal:** Allow consumers to read from both schema registries

**Steps:**
1. Add Confluent serde library dependency to consumer applications
2. Update consumer configuration to add `secondary.deserializer`
3. Deploy updated consumers (requires consumer restart)
4. Verify consumers can read messages from both registries

**Dependency changes for consumers (Maven):**
```xml
<!-- Add Confluent Schema Registry dependencies -->
<dependency>
    <groupId>io.confluent</groupId>
    <artifactId>kafka-avro-serializer</artifactId>
    <version>7.7.0</version>
</dependency>
<dependency>
    <groupId>io.confluent</groupId>
    <artifactId>kafka-schema-registry-client</artifactId>
    <version>7.7.0</version>
</dependency>
```

**Dependency changes for consumers (Gradle):**
```groovy
implementation 'io.confluent:kafka-avro-serializer:7.7.0'
implementation 'io.confluent:kafka-schema-registry-client:7.7.0'
```

**Configuration changes for consumers:**
```java
// Add these properties to existing consumer configuration:

// 1. Enable secondary deserializer
props.put(AWSSchemaRegistryConstants.SECONDARY_DESERIALIZER, 
    "io.confluent.kafka.serializers.KafkaAvroDeserializer");

// 2. Confluent SR URL
props.put("schema.registry.url", "https://<your-confluent-sr>.confluent.cloud");

// 3. Confluent SR authentication
props.put("basic.auth.credentials.source", "USER_INFO");
props.put("basic.auth.user.info", "<api-key>:<api-secret>");

// 4. Avro reader config
props.put("specific.avro.reader", "false");
```

**Downtime:** Minimal - consumers need to be restarted once

### Phase 3: Producer Migration
**Goal:** Gradually move producers from AWS Glue SR to Confluent SR

**Steps:**
1. Start with non-critical/new producers
2. Add Confluent serde library dependency (can remove Glue serde)
3. Update producer configuration to use Confluent SR serializer
4. Deploy updated producers one by one
5. Verify messages are being consumed correctly
6. Repeat until all producers are migrated

**Dependency changes for producers (Maven):**
```xml
<!-- Replace AWS Glue serde with Confluent serde -->
<dependency>
    <groupId>io.confluent</groupId>
    <artifactId>kafka-avro-serializer</artifactId>
    <version>7.7.0</version>
</dependency>
<dependency>
    <groupId>io.confluent</groupId>
    <artifactId>kafka-schema-registry-client</artifactId>
    <version>7.7.0</version>
</dependency>

<!-- Remove AWS Glue serde -->
<!-- <dependency>
    <groupId>software.amazon.glue</groupId>
    <artifactId>schema-registry-serde</artifactId>
</dependency> -->
```

**Configuration changes for producers:**
```java
// Replace Glue serializer with Confluent serializer
props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, 
    "io.confluent.kafka.serializers.KafkaAvroSerializer");

// Confluent SR configuration
props.put("schema.registry.url", "https://<your-confluent-sr>.confluent.cloud");
props.put("basic.auth.credentials.source", "USER_INFO");
props.put("basic.auth.user.info", "<api-key>:<api-secret>");
props.put("auto.register.schemas", "false");  // Use pre-registered schemas
props.put("use.latest.version", "true");
```
**Note:** Make sure all config properties related to AWS Glue SR have been accounted for before making this change. 

**Downtime:** Minimal - producers can be migrated independently

### Phase 4: Complete Migration
**Goal:** Remove AWS Glue SR dependency entirely

**Steps:**
1. Verify all producers are using Confluent SR
2. Verify no more Glue SR messages are being produced
3. Remove AWS Glue serde dependency from consumers
4. Update consumers to use Confluent SR deserializer directly
5. Remove `secondary.deserializer` configuration
6. Decommission AWS Glue SR

**Dependency changes for consumers (Maven):**
```xml
<!-- Remove AWS Glue serde, keep only Confluent -->
<dependency>
    <groupId>io.confluent</groupId>
    <artifactId>kafka-avro-serializer</artifactId>
    <version>7.7.0</version>
</dependency>
<dependency>
    <groupId>io.confluent</groupId>
    <artifactId>kafka-schema-registry-client</artifactId>
    <version>7.7.0</version>
</dependency>

<!-- Remove AWS Glue serde -->
<!-- <dependency>
    <groupId>software.amazon.glue</groupId>
    <artifactId>schema-registry-serde</artifactId>
</dependency> -->
```

**Final consumer configuration:**
```java
// Replace Glue deserializer with Confluent deserializer
props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, 
    "io.confluent.kafka.serializers.KafkaAvroDeserializer");

// Confluent SR configuration (same as before)
props.put("schema.registry.url", "https://<your-confluent-sr>.confluent.cloud");
props.put("basic.auth.credentials.source", "USER_INFO");
props.put("basic.auth.user.info", "<api-key>:<api-secret>");
```

**Downtime:** Minimal - consumers need final restart

---

## Migration Timeline Summary

| Phase | Action | Downtime | Duration |
|-------|--------|----------|----------|
| 1 | Copy schemas from Glue SR to Confluent SR | None | 1-2 days |
| 2 | Enable dual-read on consumers | Minimal (consumer restart) | 1-2 days |
| 3 | Migrate producers gradually | None | 1-4 weeks |
| 4 | Switch consumers to Confluent-only | Minimal (consumer restart) | 1-2 days |

---

## Pre-requisites

### 1. Confluent Cloud Setup

#### Create Kafka Topic
Create a topic named `payments` in your Confluent Cloud cluster.

#### Pre-register Schema in Confluent SR

**Important:** The Confluent producer requires the schema to be **pre-registered** before sending messages.

**Option A: Via Confluent Cloud Console**
1. Go to **Confluent Cloud** → **Schema Registry** → **Schemas**
2. Click **Add Schema**
3. Subject: `payments-value`
4. Schema Type: `Avro`
5. Paste the schema content from `src/main/avro/Payment.avsc`

**Option B: Via curl**
```bash
# Set your Confluent SR credentials
export SR_URL="https://<your-sr-endpoint>.confluent.cloud"
export SR_API_KEY="<your-sr-api-key>"
export SR_API_SECRET="<your-sr-api-secret>"

# Register the schema
curl -X POST "$SR_URL/subjects/payments-value/versions" \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -u "$SR_API_KEY:$SR_API_SECRET" \
  -d @- << 'EOF'
{
  "schemaType": "AVRO",
  "schema": "{\"type\":\"record\",\"name\":\"Payment\",\"namespace\":\"com.demo.migration.model\",\"doc\":\"Represents a payment transaction from PoS systems\",\"fields\":[{\"name\":\"transactionId\",\"type\":\"string\",\"doc\":\"Unique identifier for the payment transaction\"},{\"name\":\"orderId\",\"type\":\"string\",\"doc\":\"Associated order identifier\"},{\"name\":\"customerId\",\"type\":\"string\",\"doc\":\"Customer identifier\"},{\"name\":\"amount\",\"type\":\"double\",\"doc\":\"Payment amount in the specified currency\"},{\"name\":\"currency\",\"type\":\"string\",\"default\":\"USD\",\"doc\":\"Currency code (ISO 4217)\"},{\"name\":\"paymentMethod\",\"type\":{\"type\":\"enum\",\"name\":\"PaymentMethod\",\"symbols\":[\"CREDIT_CARD\",\"DEBIT_CARD\",\"DIGITAL_WALLET\",\"BANK_TRANSFER\",\"CASH\"]},\"doc\":\"Method of payment\"},{\"name\":\"paymentStatus\",\"type\":{\"type\":\"enum\",\"name\":\"PaymentStatus\",\"symbols\":[\"PENDING\",\"AUTHORIZED\",\"CAPTURED\",\"DECLINED\",\"REFUNDED\",\"CANCELLED\"]},\"doc\":\"Current status of the payment\"},{\"name\":\"posChannel\",\"type\":{\"type\":\"enum\",\"name\":\"PosChannel\",\"symbols\":[\"WEB\",\"MOBILE_APP\",\"IN_STORE\",\"CALL_CENTER\",\"KIOSK\"]},\"doc\":\"Point of Sale channel where transaction originated\"},{\"name\":\"merchantId\",\"type\":\"string\",\"doc\":\"Merchant identifier\"},{\"name\":\"merchantName\",\"type\":\"string\",\"doc\":\"Merchant business name\"},{\"name\":\"cardLastFour\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"Last four digits of card (if card payment)\"},{\"name\":\"timestamp\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"},\"doc\":\"Transaction timestamp in milliseconds since epoch\"},{\"name\":\"metadata\",\"type\":{\"type\":\"map\",\"values\":\"string\"},\"default\":{},\"doc\":\"Additional key-value metadata\"}]}"
}
EOF
```

### 2. AWS Glue Schema Registry

Ensure you have:
- AWS Glue Schema Registry created
- `Payment` schema registered
- IAM user with Glue SR read permissions

### 3. Configure Credentials

Copy the template and fill in your credentials:

```bash
cp src/main/resources/secrets.properties.template src/main/resources/secrets.properties
```

Edit `src/main/resources/secrets.properties`:

```properties
# Confluent Cloud Kafka Cluster
kafka.api.key=<YOUR_KAFKA_API_KEY>
kafka.api.secret=<YOUR_KAFKA_API_SECRET>
kafka.bootstrap.servers=<YOUR_BOOTSTRAP_SERVER>

# Confluent Schema Registry
confluent.sr.url=<YOUR_CONFLUENT_SR_URL>
confluent.sr.api.key=<YOUR_SR_API_KEY>
confluent.sr.api.secret=<YOUR_SR_API_SECRET>

# AWS Credentials
aws.access.key.id=<YOUR_AWS_ACCESS_KEY_ID>
aws.secret.access.key=<YOUR_AWS_SECRET_ACCESS_KEY>
aws.region=us-east-2

# AWS Glue Schema Registry
aws.glue.registry.name=payments-registry
```

---

## Key Concept: `secondary.deserializer`

AWS Glue Schema Registry provides a **`secondary.deserializer`** configuration that enables seamless migration:

```java
// PRIMARY: AWS Glue SR Deserializer
props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, 
    GlueSchemaRegistryKafkaDeserializer.class.getName());

// SECONDARY: Confluent SR Deserializer (for migration!)
props.put(AWSSchemaRegistryConstants.SECONDARY_DESERIALIZER, 
    KafkaAvroDeserializer.class.getName());
```

### How It Works

1. **Magic Byte Detection**: The deserializer checks the first byte of each message:
   - `0x03` → AWS Glue SR wire format → Use primary deserializer
   - `0x00` → Confluent SR wire format → Use secondary deserializer

2. **Automatic Routing**: Based on the magic byte, messages are automatically routed to the correct deserializer

3. **Schema ID Extraction**:
   - Glue: 16-byte UUID after magic byte (schema version ID)
   - Confluent: 4-byte integer after magic byte (schema ID)

---

## Running the Demo

### Build

```bash
mvn clean package -DskipTests
```

### Test AWS Connection

```bash
JAVA_TOOL_OPTIONS="-Djava.security.manager=allow" mvn exec:java \
  -Dexec.mainClass="com.demo.migration.TestGlueConnection" -q
```

### Java 17+ Compatibility

**Important**: Java 17+ requires a security manager flag for Kafka SASL authentication:

```bash
export JAVA_TOOL_OPTIONS="-Djava.security.manager=allow"
```

Or add it inline with each command as shown below.

### Run Full Demo

```bash
# Run with 5 messages per producer (10 total)
JAVA_TOOL_OPTIONS="-Djava.security.manager=allow" mvn exec:java \
  -Dexec.mainClass="com.demo.migration.DemoRunner" -Dexec.args="full 5" -q
```

### Run Individual Components

```bash
# Glue producer only (Web POS)
JAVA_TOOL_OPTIONS="-Djava.security.manager=allow" mvn exec:java \
  -Dexec.mainClass="com.demo.migration.DemoRunner" -Dexec.args="producer-glue 10" -q

# Confluent producer only (Mobile POS)
JAVA_TOOL_OPTIONS="-Djava.security.manager=allow" mvn exec:java \
  -Dexec.mainClass="com.demo.migration.DemoRunner" -Dexec.args="producer-confluent 10" -q

# Consumer only (reads both)
JAVA_TOOL_OPTIONS="-Djava.security.manager=allow" mvn exec:java \
  -Dexec.mainClass="com.demo.migration.DemoRunner" -Dexec.args="consumer" -q
```

---

## Project Structure

```
.
├── .gitignore                      # Excludes secrets.properties
├── LICENSE                         # Apache 2.0
├── README.md
├── pom.xml
└── src/main/
    ├── avro/
    │   └── Payment.avsc            # Avro schema definition
    └── resources/
        ├── secrets.properties.template  # Template for credentials
        ├── secrets.properties           # Your credentials (gitignored)
        ├── glue-producer.properties     # Web POS config
        ├── confluent-producer.properties # Mobile POS config
        └── consumer.properties          # Analytics consumer config

src/main/java/com/demo/migration/
├── DemoRunner.java                 # Main entry point
├── TestGlueConnection.java         # AWS connection test
├── config/KafkaConfig.java         # Configuration loader
├── producer/
│   ├── GluePaymentProducer.java    # AWS Glue SR producer
│   └── ConfluentPaymentProducer.java # Confluent SR producer
└── consumer/
    ├── PaymentAnalyticsConsumer.java # Dual deserializer consumer
    └── SchemaIdCapturingDeserializer.java # Extracts schema IDs from wire format
```

---

## References

- [AWS Glue Schema Registry - Migration Guide](https://docs.aws.amazon.com/glue/latest/dg/schema-registry-integrations-migration.html)
- [Confluent Schema Registry Documentation](https://docs.confluent.io/cloud/current/sr/index.html)

## License

Apache License 2.0
