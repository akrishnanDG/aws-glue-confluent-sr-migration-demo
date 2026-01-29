package com.demo.migration;

import com.demo.migration.config.KafkaConfig;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.*;

import java.util.Properties;

/**
 * Test AWS Glue Schema Registry connection
 */
public class TestGlueConnection {
    
    public static void main(String[] args) {
        Properties secrets = KafkaConfig.getSecrets();
        
        String accessKey = secrets.getProperty("aws.access.key.id");
        String secretKey = secrets.getProperty("aws.secret.access.key");
        String region = secrets.getProperty("aws.region");
        String registryName = secrets.getProperty("aws.glue.registry.name");
        
        System.out.println("=== Testing AWS Glue Schema Registry Connection ===");
        System.out.println("Region: " + region);
        System.out.println("Registry: " + registryName);
        System.out.println("Access Key: " + accessKey.substring(0, 4) + "****");
        System.out.println();
        
        try {
            GlueClient glueClient = GlueClient.builder()
                .region(Region.of(region))
                .httpClient(UrlConnectionHttpClient.builder().build())
                .credentialsProvider(StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(accessKey, secretKey)))
                .build();
            
            // Get registry info
            System.out.println("📋 Fetching registry info...");
            GetRegistryResponse registryResponse = glueClient.getRegistry(
                GetRegistryRequest.builder()
                    .registryId(RegistryId.builder().registryName(registryName).build())
                    .build());
            
            System.out.println("✅ Registry found!");
            System.out.println("   Name: " + registryResponse.registryName());
            System.out.println("   ARN: " + registryResponse.registryArn());
            System.out.println("   Status: " + registryResponse.status());
            System.out.println();
            
            // List schemas in registry
            System.out.println("📋 Listing schemas in registry...");
            ListSchemasResponse schemasResponse = glueClient.listSchemas(
                ListSchemasRequest.builder()
                    .registryId(RegistryId.builder().registryName(registryName).build())
                    .build());
            
            if (schemasResponse.schemas().isEmpty()) {
                System.out.println("   (No schemas found - registry is empty)");
            } else {
                for (SchemaListItem schema : schemasResponse.schemas()) {
                    System.out.println("   📄 Schema: " + schema.schemaName());
                    System.out.println("      ARN: " + schema.schemaArn());
                    System.out.println("      Status: " + schema.schemaStatus());
                }
            }
            
            System.out.println();
            System.out.println("✅ AWS Glue Schema Registry connection successful!");
            
            glueClient.close();
            
        } catch (Exception e) {
            System.err.println("❌ Error connecting to AWS Glue: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
